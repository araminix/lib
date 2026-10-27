import type { Connection } from './connection.js';
import { NewStreamType } from './messages.js';
import { uuid } from './utils/index.js';
import { deal, Err, Ok, Result } from './utils/result.js';

/** Whether `JSON.stringify` can serialise `data` (it throws for circular structures and BigInt values). */
function isSerializable(data: unknown): boolean {
    try {
        JSON.stringify(data);
        return true;
    } catch {
        return false;
    }
}

/**
 * Errors returned by {@link Stream.read} and {@link Stream.write}.
 *
 * @example
 * ```ts
 * const message = await stream.read(15);
 * if (message.isErr()) {
 *     switch (message.e) {
 *         case StreamErr.Timeout:
 *             break; // nothing arrived within 15 ticks; the stream is still open
 *         case StreamErr.Dead:
 *             break; // closed and fully drained
 *         case StreamErr.Concurrent:
 *             break; // another read() replaced this one
 *     }
 * }
 * ```
 */
export enum StreamErr {
    /**
     * The stream is closed. For `read()`: closed and no buffered messages are left. For `write()`: closed before the message could
     * be handed to the socket (including writes still queued when it closed).
     */
    Dead = 'Dead',
    /** Another `read()` on the same stream replaced this one; only one read can wait at a time. */
    Concurrent = 'Concurrent',
    /** `read(hp)` received nothing within `hp` ticks. The stream stays open and can be read again. */
    Timeout = 'Timeout',
    /**
     * `write()` was given data that `JSON.stringify` cannot serialise (for example a circular object or a `BigInt`). Nothing was
     * sent or queued, and the stream stays open.
     */
    Invalid = 'Invalid',
}

/**
 * Errors returned when opening a stream with `Socket.outgoingStream()` or `Connection.newOutgoingStream()`.
 *
 * @example
 * ```ts
 * const opened = await socket.outgoingStream('reports', { month: '2026-10' });
 * if (opened.isErr() && opened.e === InitStreamErr.Dead) {
 *     console.warn('stream could not be opened');
 * }
 * ```
 */
export enum InitStreamErr {
    /**
     * The stream could not be opened: the connection is dead, the peer rejected or closed the request (for example because
     * nothing acquired the name in time), or the peer did not accept it within 20 ticks.
     */
    Dead = 'Dead',
    /** Reserved: not produced by the current implementation, where every failed opening reports {@link InitStreamErr.Dead}. */
    Lost = 'Lost',
}

/**
 * Codes carried by `close_stream` messages. A closed stream exposes its code as {@link Stream.closeCode} and through
 * {@link Stream.waitClose}.
 *
 * @remarks
 * Codes received from the peer are passed through without validation, so a misbehaving peer could produce other strings.
 *
 * @example
 * ```ts
 * const code = await stream.waitClose();
 * if (code !== StreamCloseCodes.Normal) console.warn(`stream ${stream.id} ended with ${code}`);
 * ```
 */
export enum StreamCloseCodes {
    /** Closed normally ({@link Stream.closeOk}). */
    Normal = 'Normal',
    /**
     * The stream was lost: the heartbeat expired, its connection was destroyed, or (for a queued request) nothing acquired it in
     * time.
     */
    Lost = 'Lost',
    /** The peer does not know the stream id, e.g. because it already closed it. */
    NotFound = 'NotFound',
    /** A `new_stream` request reused an id that is already active or queued. */
    Duplicate = 'Duplicate',
    /** Closed because of an error: {@link Stream.closeErr}, a queue overflow, or the peer's pending stream limit was reached. */
    Error = 'Error',
}

/**
 * A bidirectional, typed message channel multiplexed with other streams over one {@link Connection}.
 *
 * You get streams from `Socket.outgoingStream()` (you open it) or `Socket.incomingStream()` (the peer opened it). Each stream has an
 * {@link Stream.id}, the {@link Stream.args} it was opened with, and two bounded FIFO queues:
 *
 * - **Incoming:** messages that arrive while no `read()` is waiting are buffered, up to {@link Stream.incomingQueueCap} (default
 *   256). One more closes the stream with {@link StreamCloseCodes.Error}. Buffered messages stay readable after the stream closed.
 * - **Outgoing:** `write()` sends immediately when it can and otherwise queues the message, up to {@link Stream.outgoingQueueCap}
 *   (default 256), until the socket is back or the heartbeat is answered. One more closes the stream with
 *   {@link StreamCloseCodes.Error}.
 *
 * Liveness is checked automatically with heartbeats counted in ticks of the connection's 1-second timer. The side that accepted
 * the stream sends `ping_stream` after 10 ticks without receiving anything and closes the stream as `Lost` after another 10; the
 * side that opened it answers with `pong_stream` and gives up after 20 ticks without receiving anything.
 *
 * Messages must be JSON-serialisable. There is no end-to-end flow control: `write()` resolves when the frame was handed to the
 * WebSocket, not when the peer read it.
 *
 * @typeParam TArgs - Type of the arguments the stream was opened with.
 * @typeParam TDataIn - Type of the messages read from the stream.
 * @typeParam TDataOut - Type of the messages written to the stream.
 *
 * @example
 * ```ts
 * // `socket` is a connected Socket
 * const opened = await socket.outgoingStream<{ file: string }, string, string>('tail', { file: 'app.log' });
 * if (opened.isOk()) {
 *     const stream = opened.unwrap();
 *     await stream.write('start');
 *     while (true) {
 *         const line = await stream.read(60);
 *         if (line.isErr()) break; // Timeout, Dead or Concurrent
 *         console.log(line.unwrap());
 *     }
 *     stream.closeOk();
 * }
 * ```
 */
export class Stream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> {
    /** The controller holding this stream's state; the stream is a thin public facade over it. */
    private controller: StreamController<TArgs, TDataIn, TDataOut>;

    /**
     * Creates the public facade of a stream controller. Streams are created by the library; you do not construct them yourself.
     *
     * @param controller - The controller that owns the stream state.
     */
    constructor(controller: StreamController<TArgs, TDataIn, TDataOut>) {
        this.controller = controller;
    }

    /**
     * `true` while the stream is open; `false` once either side closed it, it was lost, or its connection was destroyed.
     * Buffered incoming messages may still be readable when this is `false`.
     */
    get active(): boolean {
        return !this.controller.dead;
    }

    /**
     * Resolves when the stream closes, with its close code.
     *
     * @returns A promise of the {@link StreamCloseCodes} value (never rejects). Already closed streams resolve immediately.
     *
     * @example
     * ```ts
     * once(async () => {
     *     const code = await stream.waitClose();
     *     console.log(`stream ${stream.id} closed: ${code}`);
     * });
     * ```
     */
    waitClose(): Promise<StreamCloseCodes> {
        return this.controller.closePromise;
    }

    /** The code the stream was closed with, or `undefined` while it is open. */
    get closeCode(): StreamCloseCodes | undefined {
        return this.controller.closedCode;
    }

    /** Stream id: a UUID v4 generated by the side that opened the stream. */
    get id(): string {
        return this.controller.id;
    }

    /**
     * The arguments the stream was opened with (sent in `new_stream`). For streams you opened this is the object you passed; for
     * incoming streams it is the parsed JSON from the peer, not validated against `TArgs`.
     */
    get args(): Readonly<TArgs> {
        return this.controller.args;
    }

    /**
     * Maximum number of unread incoming messages. When a message arrives while the queue is full (and no `read()` is waiting) the
     * stream is closed with {@link StreamCloseCodes.Error}; the messages already buffered remain readable.
     *
     * @defaultValue 256
     */
    get incomingQueueCap(): number {
        return this.controller.incomingQueueCap;
    }

    /** Changes the incoming queue cap. Lowering it below the current length drops nothing; it only affects the next arrival. */
    set incomingQueueCap(cap: number) {
        this.controller.incomingQueueCap = cap;
    }

    /**
     * Maximum number of queued outgoing messages. A `write()` that would exceed it closes the stream with
     * {@link StreamCloseCodes.Error} and fails every queued write with {@link StreamErr.Dead}.
     *
     * @defaultValue 256
     */
    get outgoingQueueCap(): number {
        return this.controller.outgoingQueueCap;
    }

    /** Changes the outgoing queue cap. Lowering it below the current length drops nothing; it only affects the next write. */
    set outgoingQueueCap(cap: number) {
        this.controller.outgoingQueueCap = cap;
    }

    /** Number of received messages waiting to be read. */
    get incomingQueueLength(): number {
        return this.controller.incomingQueueLength;
    }

    /** Number of written messages waiting to be sent (their `write()` promises are still pending). */
    get outgoingQueueLength(): number {
        return this.controller.outgoingQueueLength;
    }

    /**
     * Reads the next message.
     *
     * A buffered message is returned immediately, even if the stream has already closed, so nothing that arrived is lost.
     * Otherwise the call waits for the next message, for at most `hp` ticks. Only one read can wait at a time: starting another
     * resolves the earlier one with {@link StreamErr.Concurrent}.
     *
     * @typeParam T - Expected message type, `TDataIn` by default. Not validated at runtime.
     * @param hp - Timeout in ticks (seconds) of the connection timer. Defaults to 30. Because ticks come from a shared 1-second
     * timer the effective timeout is between `hp - 1` and `hp` seconds. Use a positive integer: with `0`, a negative or a
     * fractional value the countdown never reaches exactly 0 and the read never times out.
     * @returns `Ok(message)`; `Err(StreamErr.Timeout)` if nothing arrived in time (the stream stays open);
     * `Err(StreamErr.Dead)` if the stream is closed and drained; `Err(StreamErr.Concurrent)` if another read replaced this one.
     * Never rejects.
     *
     * @example
     * ```ts
     * const reply = await stream.read<{ status: string }>(10);
     * if (reply.isOk()) {
     *     console.log(reply.unwrap().status);
     * } else if (reply.e === StreamErr.Timeout) {
     *     console.warn('no reply within 10 s');
     * }
     * ```
     */
    read<T = TDataIn>(hp: number = 30): Promise<Result<T, StreamErr>> {
        return this.controller.read<T>(hp);
    }

    /**
     * Sends a message to the peer.
     *
     * The message is sent at once when the socket is open, nothing is queued ahead of it and the heartbeat is healthy. Otherwise it
     * is appended to the outgoing queue, which is flushed in order on the next tick, whenever a frame arrives from the peer, and
     * before the next write; so queued messages go out within about a second of the socket being back. If the queue already
     * holds {@link Stream.outgoingQueueCap} messages the stream is closed with {@link StreamCloseCodes.Error}, and this write and
     * all queued writes resolve with {@link StreamErr.Dead}.
     *
     * Awaiting each write before the next one is a simple form of backpressure: the producer then waits whenever the stream is
     * disconnected.
     *
     * @typeParam T - Message type, `TDataOut` by default.
     * @param data - A JSON-serialisable message. `undefined` is allowed and is read back as `undefined`.
     * @returns `Ok` once the message was handed to the WebSocket (not when the peer received it), `Err(StreamErr.Invalid)` when
     * `JSON.stringify` cannot serialise `data` (a circular structure, a BigInt; nothing is sent or queued and the stream stays
     * open), or `Err(StreamErr.Dead)` when the stream is or becomes closed first. Never rejects.
     *
     * @example
     * ```ts
     * for (const row of rows) {
     *     const sent = await stream.write(row);
     *     if (sent.isErr()) break; // stream closed
     * }
     * stream.closeOk();
     * ```
     */
    async write<T = TDataOut>(data: T): Promise<Result<void, StreamErr>> {
        return this.controller.write(data);
    }

    /**
     * Closes the stream normally: sends `close_stream` with {@link StreamCloseCodes.Normal} and resolves {@link Stream.waitClose}.
     *
     * A waiting `read()` resolves with {@link StreamErr.Dead}; buffered incoming messages remain readable. Messages still in the
     * outgoing queue are discarded and their `write()` resolves with {@link StreamErr.Dead}, so await your writes first when they
     * must be delivered. Does nothing if the stream is already closed.
     *
     * @example
     * ```ts
     * await stream.write({ done: true });
     * stream.closeOk();
     * ```
     */
    closeOk(): void {
        this.controller.close(StreamCloseCodes.Normal);
    }

    /**
     * Closes the stream with {@link StreamCloseCodes.Error}, telling the peer that something went wrong. Otherwise the same as
     * {@link Stream.closeOk}.
     *
     * @example
     * ```ts
     * const request = await stream.read<{ amount: number }>();
     * if (request.isErr() || !(request.unwrap().amount > 0)) {
     *     stream.closeErr(); // the peer sees StreamCloseCodes.Error
     * }
     * ```
     */
    closeErr(): void {
        this.controller.close(StreamCloseCodes.Error);
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
    /** Writes waiting to be sent (FIFO), each with the resolver of its `write()` promise. */
    private outgoingQueue: Array<{ data: unknown; resolve: (val: Result<void, StreamErr>) => void }> = [];
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
     * @returns `Ok` once the message was handed to the socket, `Err(StreamErr.Invalid)` if `data` cannot be serialised, or
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
        if (this.outgoingQueue.length === 0 && !this._weak) {
            const result = this.connection.writeData(this.id, data);
            if (result.isOk()) {
                resolve(new Ok(undefined));
                return promise;
            }
        }
        // Checked only on the queueing path: queued data is retried until it is sent, so unserialisable data would block the
        // queue for good.
        if (!isSerializable(data)) {
            resolve(new Err(StreamErr.Invalid));
            return promise;
        }
        if (this.outgoingQueue.length >= this._outgoingQueueCap) {
            this.close(StreamCloseCodes.Error);
            resolve(new Err(StreamErr.Dead));
            return promise;
        }
        this.outgoingQueue.push({ data, resolve });
        return promise;
    }

    /** Sends queued writes in order until one fails, the stream closes or it turns weak. */
    private flushOutgoing(): void {
        if (this.dead || this._weak) {
            return;
        }
        while (this.outgoingQueue.length > 0) {
            const head = this.outgoingQueue[0];
            const result = this.connection.writeData(this.id, head.data);
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

/**
 * Controller of a stream that the peer opened and this side accepted.
 *
 * It takes its id and arguments from the `new_stream` message and actively checks the peer: after 10 ticks without receiving
 * anything it pings every 2 ticks, and closes as `Lost` after 10 more (strong controller, `hp` 10). {@link Connection} creates it
 * when an `acquireStream()` call is matched with a queued request. Not re-exported from the package entry point.
 *
 * @typeParam TArgs - Type of the stream arguments.
 * @typeParam TDataIn - Type of the messages read from the stream.
 * @typeParam TDataOut - Type of the messages written to the stream.
 *
 * @example
 * ```ts
 * // What Connection does internally once a new_stream request is accepted (it also registers the controller by id):
 * const controller = new IncomingStreamController(connection, { type: 'new_stream', id: 's-1', name: 'chat', args: { room: 1 } });
 * controller.stream.id; // 's-1'
 * controller.stream.args; // { room: 1 }
 * ```
 */
export class IncomingStreamController<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> extends StreamController<TArgs, TDataIn, TDataOut> {
    /** The `new_stream` message this stream was opened with. */
    message: NewStreamType;
    /** Heartbeat budget: 10 ticks before pinging, then 10 more before giving up. */
    protected hp: number = 10;
    /** Strong: pings the peer before giving up. */
    protected weak: boolean = false;

    /**
     * Creates the controller for an accepted `new_stream` request.
     *
     * @param connection - The connection the stream belongs to.
     * @param message - The validated `new_stream` message (provides the id and the arguments).
     * @param incomingQueueCap - Initial incoming queue cap. Defaults to 256.
     * @param outgoingQueueCap - Initial outgoing queue cap. Defaults to 256.
     */
    constructor(connection: Connection, message: NewStreamType, incomingQueueCap: number = 256, outgoingQueueCap: number = 256) {
        super(connection, incomingQueueCap, outgoingQueueCap);
        this.message = message;
    }

    /** The `args` of the `new_stream` message, cast to `TArgs` (not validated). */
    get args(): TArgs {
        return this.message.args as TArgs;
    }

    /** The id chosen by the peer in the `new_stream` message. */
    get id(): string {
        return this.message.id;
    }
}

/**
 * Controller of a stream that this side opens.
 *
 * It generates the stream id with `uuid()`, sends `new_stream` (retrying on every tick until the frame can be sent, e.g. after a
 * reconnect) and resolves {@link OutgoingStreamController.initiate} once the peer replies with `accept_stream`. It never pings:
 * it answers the peer's pings and closes as `Lost` after 20 ticks without receiving anything (weak controller, `hp` 20), which
 * also bounds how long opening may take. Not re-exported from the package entry point; use `Socket.outgoingStream()`.
 *
 * @typeParam TArgs - Type of the stream arguments.
 * @typeParam TDataIn - Type of the messages read from the stream.
 * @typeParam TDataOut - Type of the messages written to the stream.
 *
 * @example
 * ```ts
 * // Connection.newOutgoingStream() creates the controller, registers it by id and calls initiate():
 * const opened = await connection.newOutgoingStream('svc', { a: 1 });
 * if (opened.isOk()) {
 *     await opened.unwrap().write('hello');
 * }
 * ```
 */
export class OutgoingStreamController<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> extends StreamController<TArgs, TDataIn, TDataOut> {
    /** The stream name sent in `new_stream`. */
    private readonly name: string;
    /** The arguments sent in `new_stream`; the caller's object, not copied or frozen. */
    readonly args: TArgs;
    /** Heartbeat budget: 20 ticks without receiving anything before giving up. */
    protected hp: number = 20;
    /** Weak: never pings, only answers pings. */
    protected weak: boolean = true;
    /** The stream id, a fresh UUID v4. */
    readonly id: string = uuid();
    /** Resolver of the pending {@link OutgoingStreamController.initiate} promise, until the stream is accepted or destroyed. */
    private initResolve?: (val: Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>) => void;
    /** Whether `new_stream` has been sent successfully. */
    private initSend: boolean = false;

    /**
     * Creates the controller. Nothing is sent until {@link OutgoingStreamController.initiate} is called.
     *
     * @param connection - The connection the stream belongs to. The controller must also be registered in it (see
     * `Connection.newOutgoingStream()`) to receive `accept_stream`.
     * @param name - Stream name the peer acquires with `incomingStream(name)`.
     * @param args - JSON-serialisable arguments sent with `new_stream`.
     * @param incomingQueueCap - Initial incoming queue cap. Defaults to 256.
     * @param outgoingQueueCap - Initial outgoing queue cap. Defaults to 256.
     */
    constructor(connection: Connection, name: string, args: TArgs, incomingQueueCap: number = 256, outgoingQueueCap: number = 256) {
        super(connection, incomingQueueCap, outgoingQueueCap);
        this.name = name;
        this.args = args;
    }

    /**
     * Sends `new_stream` (now, or on a later tick if the socket cannot send yet) and waits for the peer's answer.
     *
     * @returns `Ok(stream)` when the peer accepts, or `Err(InitStreamErr.Dead)` when the stream is closed first (rejected by the
     * peer, 20 ticks without an answer, or the connection was destroyed). Never rejects.
     */
    initiate(): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>> {
        const [promise, resolve] = deal<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>>();
        this.initResolve = resolve;
        this.tryInitiate();
        return promise;
    }

    /** Sends `new_stream` unless it was already sent successfully. */
    private tryInitiate(): void {
        if (!this.initSend) {
            const result = this.connection.writeNewStream(this.id, this.name, this.args);
            if (result.isOk()) {
                this.initSend = true;
            }
        }
    }

    /** Handles `accept_stream`: refreshes the heartbeat and resolves the pending {@link OutgoingStreamController.initiate} call. */
    onAcceptStream(): void {
        if (this.initResolve) {
            this.restoreHp();
            this.initResolve(new Ok(this.stream));
            this.initResolve = undefined;
        }
    }

    /** Resolves a still pending {@link OutgoingStreamController.initiate} call with {@link InitStreamErr.Dead}. */
    protected onDestroy(): void {
        if (this.initResolve) {
            this.initResolve(new Err(InitStreamErr.Dead));
            this.initResolve = undefined;
        }
    }

    /** Retries sending `new_stream` on every tick until it succeeded once. */
    protected tic(): void {
        this.tryInitiate();
    }
}
