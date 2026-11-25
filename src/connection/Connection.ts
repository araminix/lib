import { SocketErr } from '../socket/types.js';
import { IncomingStreamController, InitStreamErr, OutgoingStreamController, Stream, StreamCloseCodes as StreamCloseCode, StreamController } from '../stream/index.js';
import { logger } from '../utils/logger/index.js';
import { deal, err, Err, ok, Ok, R, Result } from '../utils/result.js';
import { BackpressureMonitor } from './backpressure.js';
import { dispatchIncomingFrame, normalizeStreamCloseCode, parseIncomingMessage } from './frameRouter.js';
import { StreamRequestQueue } from './streamQueue.js';
import { CloseCodes, ConnectionInterface, DEFAULT_BACKPRESSURE_HP, DEFAULT_HP, DEFAULT_MAX_BUFFERED_BYTES, MinimalWebSocket, WsCloseCodes } from './types.js';

/**
 * One logical endpoint connection: many {@link Stream | streams} multiplexed over a WebSocket that can be replaced.
 *
 * A `Connection` outlives individual WebSockets. {@link Connection.connect} attaches a socket (closing the previous one with
 * 4409); when that socket drops, the connection keeps its streams and queued requests until the next socket is attached, so
 * reconnects are transparent to the application (resumption). If no socket is attached for {@link ConnectionInterface.hp} ticks
 * (default 300, i.e. 5 minutes) the connection destroys itself with {@link CloseCodes.Died}.
 *
 * A 1-second interval timer drives every timeout of the protocol, called "ticks" or "hp" throughout this package: the connection
 * hp, the lifetime of queued stream requests, stream heartbeats and `Stream.read()` timeouts. The timer is `unref()`ed in Node.js,
 * so it never keeps the process alive.
 *
 * Applications normally work with a `Socket`, which wraps a connection. On the client, `Client.connect()` creates and drives the
 * connection; servers such as `@araminix/platform` create one per endpoint client and attach each incoming WebSocket to it.
 *
 * @example
 * ```ts
 * import { CloseCodes, Connection, Socket } from '@araminix/lib';
 *
 * // Server side, with `ws` coming from your HTTP upgrade handler:
 * const connection = new Connection(
 *     {
 *         maxPendingStreams: 64,
 *         onAccept: () => {},
 *         onDisconnect: (code) => console.log('socket closed with', code),
 *         onDestroy: () => console.log('connection gone'),
 *     },
 *     arg,
 * );
 * connection.connect(ws);
 * if (connection.accept().isOk()) {
 *     const socket = new Socket(connection);
 *     // serve socket.incomingStream(...) here
 * }
 * ```
 */
export class Connection {
    /** Callbacks and limits supplied by the owner. */
    private readonly inter: ConnectionInterface;
    /** The endpoint argument this connection belongs to (the `<arg>` of `/ws/<name>/<arg>`), or `''`. */
    arg: string;
    /** The WebSocket currently attached, or `undefined` while disconnected. Set by {@link Connection.connect}, cleared when it closes. */
    ws: MinimalWebSocket | undefined = undefined;
    /** Ticks left without an attached socket before the connection dies; refilled when a socket opens. */
    private hp: number;
    /** Incoming `new_stream` requests that were not acquired yet, by stream name (FIFO per name). */
    private readonly streamQueue = new StreamRequestQueue();
    /** Pending {@link Connection.acquireStream} resolvers, at most one per stream name. */
    private promises: Map<string, (result: Result<Stream<any, any, any>, SocketErr>) => void> = new Map();
    /** Live stream controllers (incoming and outgoing) by stream id. */
    private streams: Map<string, StreamController<any, any, any>> = new Map();
    /** Whether `accept_endpoint` was sent (server) or received (client); later `accept_endpoint` frames are ignored. */
    private accepted: boolean = false;
    /** Resolves with the reason once the connection is destroyed. Never rejects. */
    readonly closePromise: Promise<CloseCodes>;
    /** Resolver of {@link Connection.closePromise}. */
    private resolveClose: (code: CloseCodes) => void;
    /** `true` once {@link Connection.destroy} ran. A dead connection never comes back. */
    dead: boolean = false;
    /** Tracks sustained WebSocket send-buffer backpressure and closes stalled sockets. */
    private readonly backpressure = new BackpressureMonitor();
    /** The 1-second tick timer; unref'd, and cleared on destroy. */
    private timer: NodeJS.Timeout = setInterval(() => {
        this.tic();
    }, 1000);

    /**
     * Creates a connection without a socket; attach one with {@link Connection.connect}. The hp countdown starts right away.
     *
     * @param inter - Owner callbacks and limits; see {@link ConnectionInterface}.
     * @param arg - Endpoint argument, exposed as {@link Connection.arg} and `Socket.arg`.
     *
     * @example
     * ```ts
     * const connection = new Connection({ onDestroy: () => {}, onDisconnect: () => {}, onAccept: () => {} }, 'session-1');
     * ```
     */
    constructor(inter: ConnectionInterface, arg: string) {
        this.inter = inter;
        this.arg = arg;
        this.hp = inter.hp ?? DEFAULT_HP;
        [this.closePromise, this.resolveClose] = deal<CloseCodes>();
        if (this.timer && typeof this.timer.unref === 'function') {
            this.timer.unref();
        }
    }

    /**
     * Forgets a stream by id. Stream controllers call this when they close; applications do not need it.
     *
     * @param id - The stream id. Unknown ids are ignored.
     */
    removeStream(id: string): void {
        this.streams.delete(id);
    }

    /**
     * Opens a stream called `name` towards the peer; the implementation behind `Socket.outgoingStream()`.
     *
     * Sends `new_stream` with `args` (retrying on every tick while the socket cannot send) and resolves when the peer replies with
     * `accept_stream`.
     *
     * @typeParam TArgs - Type of `args`.
     * @typeParam TDataIn - Type of the messages the peer sends.
     * @typeParam TDataOut - Type of the messages you write.
     * @param name - Stream name; the peer must acquire it with `incomingStream(name)`.
     * @param args - JSON-serializable arguments, delivered to the peer as `Stream.args`.
     * @returns `Ok(stream)` once accepted, or `Err(InitStreamErr.Dead)` if the connection is dead, the peer closed the request,
     * or it was not accepted within 20 ticks. Never rejects.
     *
     * @example
     * ```ts
     * const opened = await connection.newOutgoingStream<{ room: string }, string, string>('chat', { room: 'general' });
     * if (opened.isOk()) await opened.unwrap().write('hello');
     * ```
     */
    async newOutgoingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string, args: TArgs): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>> {
        if (this.dead) {
            return new Err(InitStreamErr.Dead);
        }
        const stream = new OutgoingStreamController<TArgs, TDataIn, TDataOut>(this, name, args);
        this.streams.set(stream.id, stream);
        return await stream.initiate();
    }

    /**
     * Server side: accepts the endpoint by sending `accept_endpoint`, which makes the client's `connect()` resolve with a `Socket`.
     *
     * May be sent again after a new socket was attached (the client ignores repeated acceptances).
     *
     * @returns `ok()` when the frame was sent. `err()` if the connection is dead, or if the frame could not be sent, in which case
     * the connection is destroyed with {@link CloseCodes.Unexpected}.
     *
     * @example
     * ```ts
     * connection.connect(ws);
     * if (connection.accept().isOk()) {
     *     const socket = new Socket(connection);
     * }
     * ```
     */
    accept(): R {
        if (this.dead) {
            return err();
        }
        const result = this.write({ type: 'accept_endpoint' });
        if (result.isErr()) {
            this.destroy(CloseCodes.Unexpected);
            return result;
        }
        this.accepted = true;
        return result;
    }

    /**
     * Server side: refuses the endpoint. Closes the socket with 4403 ({@link WsCloseCodes.Denied}) and destroys the connection
     * with {@link CloseCodes.Unauthorized}; the client's `connect()` resolves with `ConnectFail.Denied`.
     *
     * @example
     * ```ts
     * if (!allowed) {
     *     connection.deny();
     * }
     * ```
     */
    deny(): void {
        this.ws?.close(WsCloseCodes.Denied);
        this.destroy(CloseCodes.Unauthorized);
    }

    /**
     * Sends an `accept_stream` frame. Low-level: sent for you when an incoming stream is acquired.
     *
     * @param streamId - Id of the `new_stream` request being accepted.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writeAcceptStream(streamId: string): R {
        return this.write({ type: 'accept_stream', id: streamId });
    }

    /**
     * Sends a `new_stream` frame. Low-level: use {@link Connection.newOutgoingStream} to open streams.
     *
     * @typeParam TArgs - Type of `args`.
     * @param streamId - Id of the new stream.
     * @param name - Stream name.
     * @param args - JSON-serializable stream arguments. `undefined` is sent as `null`.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writeNewStream<TArgs = unknown>(streamId: string, name: string, args: TArgs): R {
        return this.write({
            type: 'new_stream',
            id: streamId,
            name,
            // JSON.stringify drops undefined properties, and a new_stream frame without `args` fails validation on the peer.
            args: args === undefined ? null : args,
        });
    }

    /**
     * Sends one `data_stream` frame. Low-level: `Stream.write()` adds queueing and heartbeat handling on top of it.
     *
     * @typeParam T - Payload type; must be JSON-serializable.
     * @param streamId - Target stream id.
     * @param data - The payload. `undefined` is allowed (the `data` key is then omitted on the wire).
     * @returns `ok()` when the frame was passed to `ws.send()`. `err()` when `data` cannot be serialized with `JSON.stringify`, no
     * socket is attached, the socket's `readyState` is not 1 (open), or `send()` threw; in the last case the socket is closed
     * (without a code) so that the usual disconnect handling takes over.
     *
     * @example
     * ```ts
     * if (connection.writeData(streamId, { tick: 1 }).isErr()) {
     *     // not sent: no open socket, or the payload is not serializable
     * }
     * ```
     */
    writeData<T = unknown>(streamId: string, data: T): R {
        return this.write({ type: 'data_stream', id: streamId, data });
    }

    /**
     * Sends a `close_stream` frame. Low-level: `Stream.closeOk()` / `Stream.closeErr()` also close the local side.
     *
     * @param streamId - Id of the stream being closed.
     * @param code - The close code to report.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writeClose(streamId: string, code: StreamCloseCode): R {
        return this.write({ type: 'close_stream', id: streamId, code });
    }

    /**
     * Sends a `ping_stream` heartbeat frame. Low-level: stream controllers send it when a stream is idle.
     *
     * @param streamId - Id of the stream to probe.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writePing(streamId: string): R {
        return this.write({ type: 'ping_stream', id: streamId });
    }

    /**
     * Sends a `pong_stream` heartbeat answer. Low-level: stream controllers send it in reply to a ping.
     *
     * @param streamId - Id of the pinged stream.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writePong(streamId: string): R {
        return this.write({ type: 'pong_stream', id: streamId });
    }

    /**
     * Sends a pre-serialized JSON frame string over the attached WebSocket.
     *
     * Used internally by stream controllers so queued messages are serialized only once. Checks socket readiness and
     * {@link ConnectionInterface.maxBufferedBytes} backpressure before calling `ws.send()`.
     *
     * @param payload - A JSON string representing one protocol frame.
     * @returns `ok()` when the frame was passed to `ws.send()`, or `err()` if no open socket is attached, the backpressure
     * threshold would be exceeded, or `ws.send()` threw.
     */
    writeRaw(payload: string): R {
        const ws = this.ws;
        if (!ws || (ws.readyState !== undefined && ws.readyState !== 1)) {
            return err();
        }
        const maxBufferedBytes = this.inter.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES;
        if (this.backpressure.wouldOverflow(ws, payload, maxBufferedBytes)) {
            return err();
        }
        try {
            ws.send(payload);
        } catch {
            this.backpressure.reset();
            try {
                ws.close();
            } catch {
                // ignore
            }
            return err();
        }
        this.backpressure.reset();
        return ok();
    }

    /** Serializes and sends one frame; see {@link Connection.writeData} for when it fails. */
    private write(message: unknown): R {
        const ws = this.ws;
        if (!ws || (ws.readyState !== undefined && ws.readyState !== 1)) {
            return err();
        }
        let payload: string;
        try {
            payload = JSON.stringify(message);
        } catch {
            return err();
        }
        return this.writeRaw(payload);
    }

    /**
     * Waits for the peer to open a stream called `name`; the implementation behind `Socket.incomingStream()`.
     *
     * A queued request for `name` is accepted as soon as `accept_stream` can be sent; otherwise the call waits for the next one.
     * Only one caller can wait per name: a new call resolves the previous one with {@link SocketErr.Concurrent}.
     *
     * @typeParam TArgs - Type of the stream arguments (not validated).
     * @typeParam TDataIn - Type of the messages the peer sends.
     * @typeParam TDataOut - Type of the messages you write.
     * @param name - The stream name to accept.
     * @param signal - Optional `AbortSignal` to cancel waiting while keeping the connection alive.
     * @returns `Ok(stream)`, `Err(SocketErr.Closed)` if the connection is or becomes dead, `Err(SocketErr.Concurrent)` if a
     * newer call for the same name replaced this one, or `Err(SocketErr.Cancelled)` if aborted via `signal` or
     * {@link Connection.cancelAcquireStream}. Never rejects.
     *
     * @example
     * ```ts
     * const incoming = await connection.acquireStream<{ room: string }>('chat');
     * if (incoming.isOk()) console.log('peer joined', incoming.unwrap().args.room);
     * ```
     */
    acquireStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string, signal?: AbortSignal): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>> {
        if (signal?.aborted) {
            return Promise.resolve(new Err(SocketErr.Cancelled));
        }
        const [promise, resolve] = deal<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>>();
        if (this.dead) {
            resolve(new Err(SocketErr.Closed));
            return promise;
        }
        const existingPromise = this.promises.get(name);
        if (existingPromise) {
            existingPromise(new Err(SocketErr.Concurrent));
        }
        let settled = false;
        const onAbort = () => {
            if (this.promises.get(name) === settle) {
                this.promises.delete(name);
            }
            settle(new Err(SocketErr.Cancelled));
        };
        const settle = (result: Result<Stream<any, any, any>, SocketErr>) => {
            if (settled) return;
            settled = true;
            signal?.removeEventListener('abort', onAbort);
            resolve(result as Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>);
        };
        this.promises.set(name, settle);
        this.roll();
        if (!settled && signal) {
            if (signal.aborted) {
                onAbort();
            } else {
                signal.addEventListener('abort', onAbort, { once: true });
            }
        }
        return promise;
    }

    /**
     * Cancels a pending {@link Connection.acquireStream} call for `name`, if one is currently waiting.
     *
     * Deletes the waiting resolver from the pending map, cleans up any attached `AbortSignal` listener, and resolves the
     * waiting promise with {@link SocketErr.Cancelled}.
     *
     * @param name - The stream name whose pending wait should be cancelled.
     * @returns `true` if a waiting call was cancelled; `false` if nobody was waiting for `name`.
     */
    cancelAcquireStream(name: string): boolean {
        const existingPromise = this.promises.get(name);
        if (!existingPromise) {
            return false;
        }
        this.promises.delete(name);
        existingPromise(new Err(SocketErr.Cancelled));
        return true;
    }

    /**
     * Pairs waiting acquirers with queued requests of the same name: sends `accept_stream` and starts the stream. Stops (and
     * re-queues the request) as soon as a frame cannot be sent.
     */
    private roll(): void {
        for (const [name, promise] of this.promises) {
            const streamRequest = this.streamQueue.shift(name);
            if (!streamRequest) {
                continue;
            }
            const result = this.writeAcceptStream(streamRequest.id);
            if (result.isErr()) {
                this.streamQueue.unshift(name, streamRequest);
                return;
            }
            const stream = this.startStream(streamRequest.message);
            this.promises.delete(name);
            promise(new Ok(stream));
        }
    }

    /** Creates and registers the controller of an accepted `new_stream` request and returns its stream. */
    private startStream(message: { id: string; name: string; args: unknown }): Stream<any, any, any> {
        const streamController = new IncomingStreamController(this, { type: 'new_stream', ...message });
        this.streams.set(streamController.id, streamController);
        return streamController.stream;
    }

    /**
     * Attaches a WebSocket, replacing the current one (which is closed with 4409, {@link WsCloseCodes.ConcurrentConnection}).
     *
     * Assigns the socket's `onopen`, `onerror`, `onclose` and `onmessage` handlers; events of a socket that has since been
     * replaced are ignored.
     * - `open`: refills hp, accepts queued stream requests and calls {@link ConnectionInterface.onOpen}.
     * - `close`: detaches the socket and calls {@link ConnectionInterface.onDisconnect} with the close code. The connection itself
     *   stays alive until its hp runs out or it is destroyed.
     * - `message`: parses the frame as JSON and dispatches it by `type`. Invalid JSON is logged and dropped; unknown types are ignored.
     *
     * If the connection is already dead, the socket is closed with 4000 ({@link WsCloseCodes.Normal}) instead.
     *
     * @param ws - The socket to attach. It may still be connecting (client) or already open (server; no `open` event follows).
     * @param reset - Refill hp right away. Defaults to `true`. The client passes `false` for reconnect attempts, so hp is only
     * refilled once the new socket actually opens.
     *
     * @example
     * ```ts
     * // Server: the client came back, so resume its existing connection on the new socket.
     * existingConnection.connect(ws);
     * existingConnection.accept();
     * ```
     */
    connect(ws: MinimalWebSocket, reset: boolean = true): void {
        if (this.dead) {
            ws.close(WsCloseCodes.Normal);
            return;
        }
        if (reset) {
            this.hp = this.inter.hp ?? DEFAULT_HP;
        }
        this.backpressure.reset();
        const oldWs = this.ws;
        this.setWs(ws);
        if (oldWs) {
            oldWs.close(WsCloseCodes.ConcurrentConnection);
        }
        ws.onopen = () => {
            if (this.ws === ws) {
                this.hp = this.inter.hp ?? DEFAULT_HP;
                this.backpressure.reset();
                this.roll();
                this.inter.onOpen?.();
            }
        };
        ws.onerror = (event: any) => {
            logger.error(`WS Connection - onError:`, event);
        };
        ws.onclose = (event: { code: number }) => {
            if (this.ws === ws) {
                this.backpressure.reset();
                this.setWs(undefined);
                this.inter.onDisconnect(event.code);
                logger.warn(`WS Connection - onClose, code: ${event.code}`);
            }
        };
        ws.onmessage = (message: { data: any }) => {
            if (this.ws !== ws) {
                return;
            }
            const parsed = parseIncomingMessage(message.data);
            if (parsed.ok) {
                this.onIncoming(parsed.data);
            }
        };
        this.roll();
    }

    /** Assigns `this.ws` with standard property descriptor flags. */
    private setWs(ws: MinimalWebSocket | undefined): void {
        Object.defineProperty(this, 'ws', {
            value: ws,
            writable: true,
            configurable: true,
            enumerable: true,
        });
    }

    /** Routes a parsed frame to its handler by message type; frames that match no codec are ignored. */
    private onIncoming(data: unknown): void {
        dispatchIncomingFrame(data, {
            onNewStream: (frame) => this.onNewStream(frame),
            onData: (frame) => this.onData(frame),
            onCloseStream: (frame) => this.onCloseStream(frame),
            onPingStream: (frame) => this.onPingStream(frame),
            onPongStream: (frame) => this.onPongStream(frame),
            onAcceptStream: (frame) => this.onAcceptStream(frame),
            onAcceptEndpoint: () => {
                if (!this.accepted) {
                    this.accepted = true;
                    this.inter.onAccept();
                }
            },
        });
    }

    /**
     * Runs once per second: drains hp while no socket is attached (destroying the connection with `Died` when it reaches 0),
     * checks for sustained WebSocket backpressure, then ticks the queued stream requests and the live streams.
     */
    private tic(): void {
        if (!this.ws) {
            this.hp--;
        }
        if (this.hp === 0) {
            this.destroy(CloseCodes.Died);
            return;
        }
        this.backpressure.check(this.ws, this.inter.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES, this.inter.backpressureHp ?? DEFAULT_BACKPRESSURE_HP);
        this.streamQueue.tick();
        this.streams.forEach((stream) => {
            stream._tic();
        });
    }

    /** Handles `accept_stream`: resolves the matching outgoing stream, or answers `close_stream` NotFound for unknown ids. */
    private onAcceptStream(data: { id: string }): void {
        const stream = this.streams.get(data.id);
        if (!stream || !(stream instanceof OutgoingStreamController)) {
            this.writeClose(data.id, StreamCloseCode.NotFound);
            return;
        }
        stream.onAcceptStream();
    }

    /** Handles `pong_stream` for a live stream, or answers `close_stream` NotFound for unknown ids. */
    private onPongStream(data: { id: string }): void {
        this.backpressure.reset();
        const stream = this.streams.get(data.id);
        if (!stream) {
            this.writeClose(data.id, StreamCloseCode.NotFound);
            return;
        }
        stream.onPong();
    }

    /** Handles `ping_stream` for a live stream, or answers `close_stream` NotFound for unknown ids. */
    private onPingStream(data: { id: string }): void {
        const stream = this.streams.get(data.id);
        if (!stream) {
            this.writeClose(data.id, StreamCloseCode.NotFound);
            return;
        }
        stream.onPing();
    }

    /** Delivers `data_stream` to a live stream, or answers `close_stream` NotFound for unknown ids. */
    private onData(data: { id: string; data: unknown }): void {
        const stream = this.streams.get(data.id);
        if (!stream) {
            this.writeClose(data.id, StreamCloseCode.NotFound);
            return;
        }
        stream.onData(data.data);
    }

    /**
     * Queues an incoming `new_stream` request for its name and tries to hand it to a waiting acquirer. Ids that are already live
     * or queued are rejected with `Duplicate`; requests beyond {@link ConnectionInterface.maxPendingStreams} with `Error`.
     */
    private onNewStream(data: { id: string; name: string; args: unknown }): void {
        if (this.streams.has(data.id) || this.streamQueue.isQueued(data.id)) {
            this.writeClose(data.id, StreamCloseCode.Duplicate);
            return;
        }
        if (this.inter.maxPendingStreams !== undefined && this.streamQueue.count() >= this.inter.maxPendingStreams) {
            this.writeClose(data.id, StreamCloseCode.Error);
            return;
        }
        this.streamQueue.enqueue(data, (id) => this.writeClose(id, StreamCloseCode.Lost), this.inter.streamRequestHp);
        this.roll();
    }

    /** Handles `close_stream`: closes the local side of the stream with the peer's code, without replying. */
    private onCloseStream(data: { id: string; code: string }): void {
        const stream = this.streams.get(data.id);
        if (!stream) {
            return;
        }
        stream.onClose(normalizeStreamCloseCode(data.code));
    }

    /**
     * Destroys the connection permanently. Calling it again has no effect.
     *
     * In this order it marks the connection dead, resolves {@link Connection.closePromise} with `closeCode`, stops the tick timer,
     * closes every live stream with `Lost` (sending `close_stream`, best effort), rejects queued stream requests with `Lost`,
     * closes the socket — with 4401 ({@link WsCloseCodes.Unauthorized}) when `closeCode` is {@link CloseCodes.Unauthorized}, so
     * the peer learns why (for example on logout), otherwise with 4000 ({@link WsCloseCodes.Normal}) — resolves waiting
     * {@link Connection.acquireStream} calls with {@link SocketErr.Closed}, and finally calls {@link ConnectionInterface.onDestroy}.
     *
     * @param closeCode - The reason, reported by {@link Connection.closePromise} and `Socket.waitClose()`.
     *
     * @example
     * ```ts
     * connection.destroy(CloseCodes.Intentional);
     * await connection.closePromise; // CloseCodes.Intentional
     * ```
     */
    destroy(closeCode: CloseCodes): void {
        if (this.dead) return;
        this.dead = true;
        this.resolveClose(closeCode);
        clearInterval(this.timer);
        this.streams.forEach((stream) => {
            stream.close(StreamCloseCode.Lost);
        });
        this.streamQueue.drainAndClear((id) => {
            this.writeClose(id, StreamCloseCode.Lost);
        });
        this.backpressure.reset();
        if (this.ws) {
            this.ws.close(closeCode === CloseCodes.Unauthorized ? WsCloseCodes.Unauthorized : WsCloseCodes.Normal);
        }
        this.promises.forEach((promise) => {
            promise(new Err(SocketErr.Closed));
        });
        this.promises.clear();
        this.inter.onDestroy();
    }
}
