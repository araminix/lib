import type { Connection } from '../connection/index.js';
import { deal, Err, Ok, R, Result } from '../utils/result.js';
import { Stream } from './Stream.js';
import { StreamCloseCodes, StreamErr } from './types.js';

/** Serializes a `data_stream` frame once; returns `undefined` if `JSON.stringify` throws (circular structures, BigInt, etc.). */
function serializeDataFrame(streamId: string, data: unknown): string | undefined {
    try {
        return JSON.stringify({
            type: 'data_stream',
            id: streamId,
            data,
        });
    } catch {
        return undefined;
    }
}

/**
 * Internal state machine behind a {@link Stream}: incoming and outgoing queues, heartbeat, read timeout and closing.
 *
 * {@link Connection} creates an {@link IncomingStreamController} or {@link OutgoingStreamController} per stream, routes the
 * stream's frames to it ({@link StreamController.onData}, {@link StreamController.onPing}, {@link StreamController.onPong},
 * {@link StreamController.onClose}) and calls {@link StreamController._tic} once per second. Applications use the
 * {@link StreamController.stream} facade instead. Not re-exported from the package entry point.
 *
 * Heartbeat: every received frame refills `hp`. When `hp` runs out, a weak controller closes as `Lost` right away; a strong one
 * enters the weak state, queues writes, pings the peer every 2 ticks and closes as `Lost` if nothing arrives for another `hp`
 * ticks.
 *
 * @typeParam TArgs - Type of the stream arguments.
 * @typeParam TDataIn - Type of the messages read from the stream.
 * @typeParam TDataOut - Type of the messages written to the stream.
 *
 * @example
 * ```ts
 * // A minimal controller, as used in unit tests with a fake connection.
 * class TestController extends StreamController<string, string, string> {
 *     protected hp = 10;
 *     protected weak = false;
 *     get id() {
 *         return 'test-stream';
 *     }
 *     get args() {
 *         return 'test-args';
 *     }
 * }
 *
 * const controller = new TestController(connection);
 * controller.onData('hello');
 * const message = await controller.stream.read(); // Ok('hello')
 * ```
 */
export abstract class StreamController<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> {
    /** The connection used to send this stream's frames. */
    protected connection: Connection;
    /** The public {@link Stream} facade bound to this controller. */
    stream: Stream<TArgs, TDataIn, TDataOut> = new Stream(this);
    /** Currently unused. */
    protected initiated: boolean = false;
    /** Remaining heartbeat ticks; `undefined` until the first tick or received frame. */
    private _hp?: number;
    /** `true` while a strong controller waits for the peer to answer its pings; writes are queued meanwhile. */
    private _weak: boolean = false;
    /** Heartbeat budget in ticks, refilled by every received frame (10 for incoming, 20 for outgoing streams). */
    protected abstract hp: number;
    /** `true` for a controller that only listens and closes as soon as `hp` runs out; `false` for one that pings first. */
    protected abstract weak: boolean;
    /** Received messages that were not read yet (FIFO). */
    private incomingQueue: unknown[] = [];
    /** Backing field of {@link StreamController.incomingQueueCap}. */
    private _incomingQueueCap: number = 256;
    /** Writes waiting to be sent (FIFO), each with its pre-serialized frame and the resolver of its `write()` promise. */
    private outgoingQueue: Array<{ payload: string; data?: unknown; resolve: (val: Result<void, StreamErr>) => void }> = [];
    /** Backing field of {@link StreamController.outgoingQueueCap}. */
    private _outgoingQueueCap: number = 256;
    /** `true` once the stream is closed. */
    dead: boolean = false;
    /** The close code once closed; `undefined` while open. */
    closedCode: StreamCloseCodes | undefined = undefined;
    /** Resolver of the single pending `read()`, if any. */
    private readResolve?: (val: Result<any, StreamErr>) => void;
    /** Ticks left before the pending `read()` times out. */
    private readHp?: number;
    /** Resolves with the close code once the stream is closed. */
    readonly closePromise: Promise<StreamCloseCodes>;
    /** Resolver of {@link StreamController.closePromise}. */
    private closeResolve: (val: StreamCloseCodes) => void;

    /**
     * Creates a controller for a stream on `connection`.
     *
     * @param connection - The connection the stream belongs to.
     * @param incomingQueueCap - Initial incoming queue cap. Defaults to 256.
     * @param outgoingQueueCap - Initial outgoing queue cap. Defaults to 256.
     */
    constructor(connection: Connection, incomingQueueCap: number = 256, outgoingQueueCap: number = 256) {
        this.connection = connection;
        this._incomingQueueCap = incomingQueueCap;
        this._outgoingQueueCap = outgoingQueueCap;
        [this.closePromise, this.closeResolve] = deal<StreamCloseCodes>();
    }

    /** The stream id. */
    abstract get id(): string;
    /** The arguments the stream was opened with. */
    abstract get args(): TArgs;

    /**
     * Maximum number of unread incoming messages; see {@link Stream.incomingQueueCap}.
     *
     * @defaultValue 256
     */
    get incomingQueueCap(): number {
        return this._incomingQueueCap;
    }

    /** Changes the incoming queue cap. */
    set incomingQueueCap(cap: number) {
        this._incomingQueueCap = cap;
    }

    /**
     * Maximum number of queued outgoing messages; see {@link Stream.outgoingQueueCap}.
     *
     * @defaultValue 256
     */
    get outgoingQueueCap(): number {
        return this._outgoingQueueCap;
    }

    /** Changes the outgoing queue cap. */
    set outgoingQueueCap(cap: number) {
        this._outgoingQueueCap = cap;
    }

    /** Number of received messages waiting to be read. */
    get incomingQueueLength(): number {
        return this.incomingQueue.length;
    }

    /** Number of writes waiting to be sent. */
    get outgoingQueueLength(): number {
        return this.outgoingQueue.length;
    }

    /** Marks the peer as alive: refills `hp`, leaves the weak state and flushes queued writes. */
    protected restoreHp(): void {
        this._weak = false;
        this._hp = this.hp;
        this.flushOutgoing();
    }

    /**
     * Closes the stream locally and tells the peer with a `close_stream` frame carrying `code` (best effort; the frame is not
     * retried). Does nothing if already closed.
     *
     * @param code - The close code to report.
     */
    close(code: StreamCloseCodes): void {
        if (this.dead) {
            return;
        }
        this.closedCode = code;
        this.connection.writeClose(this.id, code);
        this.destroy();
    }

    /**
     * Sends or queues a message; see {@link Stream.write} for the full semantics.
     *
     * @typeParam T - Message type.
     * @param data - A JSON-serialisable message.
     * @returns `Ok` once the message was handed to the socket, `Err(StreamErr.Invalid)` if `data` cannot be serialized, or
     * `Err(StreamErr.Dead)`.
     */
    write<T = TDataOut>(data: T): Promise<Result<void, StreamErr>> {
        const [promise, resolve] = deal<Result<void, StreamErr>>();
        if (this.dead) {
            resolve(new Err(StreamErr.Dead));
            return promise;
        }
        if (this.outgoingQueue.length > 0) {
            this.flushOutgoing();
        }
        if (this.dead) {
            resolve(new Err(StreamErr.Dead));
            return promise;
        }
        const payload = serializeDataFrame(this.id, data);
        if (payload === undefined) {
            resolve(new Err(StreamErr.Invalid));
            return promise;
        }
        if (this.outgoingQueue.length === 0 && !this._weak) {
            const result = this.sendDataFrame(payload, data);
            if (result.isOk()) {
                resolve(new Ok(undefined));
                return promise;
            }
        }
        if (this.outgoingQueue.length >= this._outgoingQueueCap) {
            this.close(StreamCloseCodes.Error);
            resolve(new Err(StreamErr.Dead));
            return promise;
        }
        const keepRawData = typeof this.connection.writeRaw !== 'function';
        this.outgoingQueue.push(keepRawData ? { payload, data, resolve } : { payload, resolve });
        return promise;
    }

    /** Hands a pre-serialized `data_stream` frame to the connection, falling back to `writeData` for minimal test doubles. */
    private sendDataFrame(payload: string, data: unknown): R {
        if (typeof this.connection.writeRaw === 'function') {
            return this.connection.writeRaw(payload);
        }
        return this.connection.writeData(this.id, data);
    }

    /** Sends queued writes in order until one fails, the stream closes or it turns weak. */
    private flushOutgoing(): void {
        if (this.dead || this._weak) {
            return;
        }
        while (this.outgoingQueue.length > 0) {
            const head = this.outgoingQueue[0];
            const result = this.sendDataFrame(head.payload, head.data);
            if (result.isOk()) {
                this.outgoingQueue.shift();
                head.resolve(new Ok(undefined));
                if (this.dead || this._weak) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    /**
     * Returns the next buffered message or waits for one; see {@link Stream.read} for the full semantics.
     *
     * @typeParam T - Expected message type.
     * @param hp - Timeout in ticks. Defaults to 30.
     * @returns `Ok(message)`, or `Err` with {@link StreamErr.Timeout}, {@link StreamErr.Dead} or {@link StreamErr.Concurrent}.
     */
    read<T = TDataIn>(hp: number = 30): Promise<Result<T, StreamErr>> {
        const [promise, resolve] = deal<Result<T, StreamErr>>();
        if (this.incomingQueue.length > 0) {
            const data = this.incomingQueue.shift();
            resolve(new Ok(data as T));
            return promise;
        }
        if (this.dead) {
            resolve(new Err(StreamErr.Dead));
            return promise;
        }
        this.readHp = hp;
        if (this.readResolve) {
            this.readResolve(new Err(StreamErr.Concurrent));
        }
        this.readResolve = resolve;
        return promise;
    }

    /**
     * Handles a `close_stream` from the peer: records its code and closes this side without sending anything back.
     *
     * @param code - The code the peer sent.
     */
    onClose(code: StreamCloseCodes): void {
        if (this.dead) {
            return;
        }
        this.closedCode = code;
        this.destroy();
    }

    /**
     * Handles a `data_stream` from the peer: refreshes the heartbeat, then hands the message to the waiting `read()` or buffers it.
     * If the buffer is full the stream is closed with {@link StreamCloseCodes.Error} and the message is dropped.
     *
     * @typeParam T - Message type.
     * @param data - The received message.
     */
    onData<T = TDataIn>(data: T): void {
        if (this.dead) {
            return;
        }
        this.restoreHp();
        if (this.readResolve) {
            const resolve = this.readResolve;
            this.readResolve = undefined;
            this.readHp = undefined;
            resolve(new Ok(data));
            return;
        }
        if (this.incomingQueue.length >= this._incomingQueueCap) {
            this.close(StreamCloseCodes.Error);
            return;
        }
        this.incomingQueue.push(data);
    }

    /** Handles a `pong_stream`: the peer is alive, so the heartbeat is refreshed and queued writes are flushed. */
    onPong(): void {
        this.restoreHp();
    }

    /** Handles a `ping_stream`: refreshes the heartbeat and answers with `pong_stream`. */
    onPing(): void {
        this.restoreHp();
        this.connection.writePong(this.id);
    }

    /**
     * Advances the stream by one tick; called by {@link Connection} once per second.
     *
     * Counts down the heartbeat (pinging or closing as `Lost` as described on {@link StreamController}), counts down a pending
     * read and resolves it with {@link StreamErr.Timeout} when it reaches 0, retries queued writes, then runs the subclass hook
     * {@link StreamController.tic}.
     */
    _tic(): void {
        if (this._hp === undefined) {
            this._hp = this.hp;
        }
        this._hp--;
        if (this._hp === 0 && (this.weak || this._weak)) {
            this.closedCode = StreamCloseCodes.Lost;
            this.destroy();
            return;
        }
        if (this._hp === 0 && !this._weak) {
            this._weak = true;
            this._hp = this.hp;
        }
        if (this._weak && !this.weak && this._hp % 2 === 0) {
            this.connection.writePing(this.id);
        }
        if (this.readResolve && this.readHp !== undefined) {
            this.readHp--;
            if (this.readHp === 0) {
                this.readResolve(new Err(StreamErr.Timeout));
                this.readResolve = undefined;
                this.readHp = undefined;
            }
        }
        if (!this._weak && this.outgoingQueue.length > 0) {
            this.flushOutgoing();
        }
        this.tic();
    }

    /** Per-tick hook for subclasses; does nothing by default. */
    protected tic(): void {}

    /**
     * Final teardown (once): marks the stream dead, fails a pending read and all queued writes with {@link StreamErr.Dead},
     * unregisters it from the connection, runs {@link StreamController.onDestroy} and resolves the close promise.
     */
    private destroy(): void {
        if (this.dead) {
            return;
        }
        this.dead = true;
        if (this.readResolve) {
            this.readResolve(new Err(StreamErr.Dead));
            this.readResolve = undefined;
            this.readHp = undefined;
        }
        while (this.outgoingQueue.length > 0) {
            const item = this.outgoingQueue.shift()!;
            item.resolve(new Err(StreamErr.Dead));
        }
        this.connection.removeStream(this.id);
        this.onDestroy();
        if (this.closedCode) {
            this.closeResolve(this.closedCode);
        }
    }

    /** Teardown hook for subclasses; does nothing by default. */
    protected onDestroy(): void {}
}
