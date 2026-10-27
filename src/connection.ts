import { AcceptEndpoint, AcceptStream, CloseStream, DataStream, NewStream, PingStream, PongStream } from './messages.js';
import { SocketErr } from './Socket.js';
import { IncomingStreamController, InitStreamErr, OutgoingStreamController, Stream, StreamCloseCodes as StreamCloseCode, StreamController } from './stream.js';
import { logger } from './utils/logger.js';
import { deal, err, Err, ok, Ok, R, Result } from './utils/result.js';

/**
 * The subset of the WHATWG `WebSocket` interface that a {@link Connection} needs.
 *
 * The global `WebSocket` of browsers and Node.js >= 22 satisfies it, as does the `WebSocket` of the `ws` package used on servers.
 * {@link Connection.connect} assigns the four `on*` handlers, replacing any that were set before.
 *
 * @example
 * ```ts
 * // A test double, as used in the unit tests:
 * const sent: string[] = [];
 * const ws: MinimalWebSocket = {
 *     readyState: 1,
 *     send: (data) => sent.push(data),
 *     close: () => {},
 *     onopen: null,
 *     onerror: null,
 *     onclose: null,
 *     onmessage: null,
 * };
 * connection.connect(ws);
 * ```
 */
export interface MinimalWebSocket {
    /** Sends one text frame. May throw; {@link Connection} then closes the socket and lets the reconnect logic take over. */
    send(data: string): void;
    /** Starts the closing handshake, optionally with a close code (e.g. one of {@link WsCloseCodes}) and a reason. */
    close(code?: number, reason?: string): void;
    /** Open handler; assigned by {@link Connection.connect}. */
    onopen: ((event: any) => void) | null;
    /** Error handler; assigned by {@link Connection.connect}, which only logs errors (a `close` event follows). */
    onerror: ((event: any) => void) | null;
    /** Close handler; assigned by {@link Connection.connect}, which forwards `code` to {@link ConnectionInterface.onDisconnect}. */
    onclose: ((event: { code: number; reason?: string }) => void) | null;
    /** Message handler; assigned by {@link Connection.connect}. `data` is a JSON string, or a value whose `toString()` is (e.g. a Node `Buffer`). */
    onmessage: ((event: { data: any }) => void) | null;
    /**
     * WHATWG ready state: 0 connecting, 1 open, 2 closing, 3 closed. Optional: when it is missing the socket is treated as
     * open and every send is attempted.
     */
    readyState?: number;
}

/** Default {@link ConnectionInterface.hp}: ticks a connection may spend without a socket before it dies. */
const DEFAULT_HP = 300; // seconds (60 * 5) 5 minutes
/** Default {@link ConnectionInterface.streamRequestHp}. */
const DEFAULT_STREAM_REQUEST_HP = 10; // seconds a new_stream request waits for an acquirer

/** An incoming `new_stream` request waiting for {@link Connection.acquireStream}; it loses 1 hp per tick and is cancelled at 0. */
class StreamRequest {
    /** The validated `new_stream` message. */
    message: { id: string; name: string; args: unknown };
    /** Called when hp reaches 0; removes the request from the queue and closes it with `Lost`. */
    onCancel: () => void;
    /** Ticks left before the request is cancelled. */
    hp: number;

    /** Creates a request; `hp` defaults to {@link DEFAULT_STREAM_REQUEST_HP}. */
    constructor(message: { id: string; name: string; args: unknown }, onCancel: () => void, hp: number = DEFAULT_STREAM_REQUEST_HP) {
        this.message = message;
        this.onCancel = onCancel;
        this.hp = hp;
    }

    /** Removes 1 hp (called once per tick) and cancels the request when hp reaches exactly 0. */
    damage(): void {
        this.hp--;
        if (this.hp === 0) {
            this.onCancel();
        }
    }

    /** The stream id of the request. */
    get id(): string {
        return this.message.id;
    }
}

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
    private streamQueue: Map<string, StreamRequest[]> = new Map();
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
     * @param args - JSON-serialisable arguments, delivered to the peer as `Stream.args`.
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
        const result = this.write({
            type: 'accept_endpoint',
        });
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
        const message = {
            type: 'accept_stream',
            id: streamId,
        };
        return this.write(message);
    }

    /**
     * Sends a `new_stream` frame. Low-level: use {@link Connection.newOutgoingStream} to open streams.
     *
     * @typeParam TArgs - Type of `args`.
     * @param streamId - Id of the new stream.
     * @param name - Stream name.
     * @param args - JSON-serialisable stream arguments.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writeNewStream<TArgs = unknown>(streamId: string, name: string, args: TArgs): R {
        const message = {
            type: 'new_stream',
            id: streamId,
            name,
            args,
        };
        return this.write(message);
    }

    /**
     * Sends one `data_stream` frame. Low-level: `Stream.write()` adds queueing and heartbeat handling on top of it.
     *
     * @typeParam T - Payload type; must be JSON-serialisable.
     * @param streamId - Target stream id.
     * @param data - The payload. `undefined` is allowed (the `data` key is then omitted on the wire).
     * @returns `ok()` when the frame was passed to `ws.send()`. `err()` when `data` cannot be serialised with `JSON.stringify`, no
     * socket is attached, the socket's `readyState` is not 1 (open), or `send()` threw; in the last case the socket is closed
     * (without a code) so that the usual disconnect handling takes over.
     *
     * @example
     * ```ts
     * if (connection.writeData(streamId, { tick: 1 }).isErr()) {
     *     // not sent: no open socket, or the payload is not serialisable
     * }
     * ```
     */
    writeData<T = unknown>(streamId: string, data: T): R {
        const message = {
            type: 'data_stream',
            id: streamId,
            data,
        };
        return this.write(message);
    }

    /**
     * Sends a `close_stream` frame. Low-level: `Stream.closeOk()` / `Stream.closeErr()` also close the local side.
     *
     * @param streamId - Id of the stream being closed.
     * @param code - The close code to report.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writeClose(streamId: string, code: StreamCloseCode): R {
        const message = {
            type: 'close_stream',
            id: streamId,
            code,
        };
        return this.write(message);
    }

    /**
     * Sends a `ping_stream` heartbeat frame. Low-level: stream controllers send it when a stream is idle.
     *
     * @param streamId - Id of the stream to probe.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writePing(streamId: string): R {
        const message = {
            type: 'ping_stream',
            id: streamId,
        };
        return this.write(message);
    }

    /**
     * Sends a `pong_stream` heartbeat answer. Low-level: stream controllers send it in reply to a ping.
     *
     * @param streamId - Id of the pinged stream.
     * @returns `ok()` if the frame was handed to the socket, `err()` otherwise (see {@link Connection.writeData}).
     */
    writePong(streamId: string): R {
        const message = {
            type: 'pong_stream',
            id: streamId,
        };
        return this.write(message);
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
     * @returns `Ok(stream)`, `Err(SocketErr.Closed)` if the connection is or becomes dead, or `Err(SocketErr.Concurrent)` if a
     * newer call for the same name replaced this one. Never rejects.
     *
     * @example
     * ```ts
     * const incoming = await connection.acquireStream<{ room: string }>('chat');
     * if (incoming.isOk()) console.log('peer joined', incoming.unwrap().args.room);
     * ```
     */
    acquireStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>> {
        const [promise, resolve] = deal<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>>();
        if (this.dead) {
            resolve(new Err(SocketErr.Closed));
            return promise;
        }
        const existingPromise = this.promises.get(name);
        if (existingPromise) {
            existingPromise(new Err(SocketErr.Concurrent));
        }
        this.promises.set(name, resolve as (result: Result<Stream<any, any, any>, SocketErr>) => void);
        this.roll();
        return promise;
    }

    /**
     * Pairs waiting acquirers with queued requests of the same name: sends `accept_stream` and starts the stream. Stops (and
     * re-queues the request) as soon as a frame cannot be sent.
     */
    private roll(): void {
        for (const [name, promise] of this.promises) {
            const streams = this.streamQueue.get(name);
            if (!streams) {
                continue;
            }
            const streamRequest = streams.shift();
            if (!streamRequest) {
                continue;
            }
            const result = this.writeAcceptStream(streamRequest.id);
            if (result.isErr()) {
                streams.unshift(streamRequest);
                return;
            }
            const stream = this.startStream(streamRequest.message as any);
            this.promises.delete(name);
            promise(new Ok(stream));
        }
    }

    /** Creates and registers the controller of an accepted `new_stream` request and returns its stream. */
    private startStream(message: any): Stream<any, any, any> {
        const streamController = new IncomingStreamController(this, message);
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
        const oldWs = this.ws;
        Object.defineProperty(this, 'ws', {
            value: ws,
            writable: true,
            configurable: true,
            enumerable: true,
        });
        if (oldWs) {
            oldWs.close(WsCloseCodes.ConcurrentConnection);
        }
        ws.onopen = () => {
            if (this.ws === ws) {
                this.hp = this.inter.hp ?? DEFAULT_HP;
                this.roll();
                this.inter.onOpen?.();
            }
        };
        ws.onerror = (event: any) => {
            logger.error(`WS Connection - onError:`, event);
        };
        ws.onclose = (event: { code: number }) => {
            if (this.ws === ws) {
                Object.defineProperty(this, 'ws', {
                    value: undefined,
                    writable: true,
                    configurable: true,
                    enumerable: true,
                });
                this.inter.onDisconnect(event.code);
                logger.warn(`WS Connection - onClose, code: ${event.code}`);
            }
        };
        ws.onmessage = (message: { data: any }) => {
            if (this.ws === ws) {
                try {
                    const raw = typeof message.data === 'string' ? message.data : message.data.toString();
                    const data = JSON.parse(raw);
                    logger.trace('WS Connection - onMessage:', raw);
                    this.onIncoming(data);
                } catch (error) {
                    logger.error(`WS Connection - onMessage JSON Parse Error:`, error);
                }
            }
        };
        this.roll();
    }

    /** Routes a parsed frame to its handler by message type; frames that match no codec are ignored. */
    private onIncoming(data: unknown): void {
        switch (true) {
            case NewStream.is(data): {
                this.onNewStream(data);
                break;
            }
            case DataStream.is(data): {
                this.onData(data);
                break;
            }
            case CloseStream.is(data): {
                this.onCloseStream(data);
                break;
            }
            case PingStream.is(data): {
                this.onPingStream(data);
                break;
            }
            case PongStream.is(data): {
                this.onPongStream(data);
                break;
            }
            case AcceptStream.is(data): {
                this.onAcceptStream(data);
                break;
            }
            case AcceptEndpoint.is(data): {
                if (this.accepted) {
                    break;
                }
                this.accepted = true;
                this.inter.onAccept();
                break;
            }
        }
    }

    /**
     * Runs once per second: drains hp while no socket is attached (destroying the connection with `Died` when it reaches 0), then
     * ticks the queued stream requests and the live streams.
     */
    private tic(): void {
        if (!this.ws) {
            this.hp--;
        }
        if (this.hp === 0) {
            this.destroy(CloseCodes.Died);
            return;
        }
        this.streamQueue.forEach((streams) => {
            streams.forEach((stream) => stream.damage());
        });
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
        if (this.streams.has(data.id) || this.isQueued(data.id)) {
            this.writeClose(data.id, StreamCloseCode.Duplicate);
            return;
        }
        if (this.inter.maxPendingStreams !== undefined && this.pendingStreamCount() >= this.inter.maxPendingStreams) {
            this.writeClose(data.id, StreamCloseCode.Error);
            return;
        }
        const streamRequest = new StreamRequest(
            data,
            () => {
                const requests = this.streamQueue.get(data.name);
                if (!requests) {
                    return;
                }
                const newRequests = requests.filter((request) => request.id !== data.id);
                if (newRequests.length === requests.length) {
                    return;
                }
                this.writeClose(data.id, StreamCloseCode.Lost);
                if (newRequests.length === 0) {
                    this.streamQueue.delete(data.name);
                    return;
                }
                this.streamQueue.set(data.name, newRequests);
            },
            this.inter.streamRequestHp,
        );
        if (!this.streamQueue.has(data.name)) {
            this.streamQueue.set(data.name, []);
        }
        this.streamQueue.get(data.name)!.push(streamRequest);
        this.roll();
    }

    /** Whether a request with this stream id is waiting in the queue. */
    private isQueued(id: string): boolean {
        for (const requests of this.streamQueue.values()) {
            if (requests.some((request) => request.id === id)) {
                return true;
            }
        }
        return false;
    }

    /** Number of queued (not yet acquired) stream requests across all names. */
    private pendingStreamCount(): number {
        let count = 0;
        this.streamQueue.forEach((requests) => {
            count += requests.length;
        });
        return count;
    }

    /** Serialises and sends one frame; see {@link Connection.writeData} for when it fails. */
    private write(message: unknown): R {
        let payload: string;
        try {
            payload = JSON.stringify(message);
        } catch {
            return err();
        }
        const ws = this.ws;
        if (!ws || (ws.readyState !== undefined && ws.readyState !== 1)) {
            return err();
        }
        try {
            ws.send(payload);
        } catch {
            try {
                ws.close();
            } catch {
                // ignore
            }
            return err();
        }
        return ok();
    }

    /**
     * Handles `close_stream`: closes the local side of the stream with the peer's code, without replying. Unknown ids are ignored,
     * and codes that are not a {@link StreamCloseCodes} value are reported as `Error` (an empty code would otherwise leave
     * `waitClose()` pending forever).
     */
    private onCloseStream(data: { id: string; code: string }): void {
        const stream = this.streams.get(data.id);
        if (!stream) {
            return;
        }
        const knownCode = (Object.values(StreamCloseCode) as string[]).includes(data.code);
        stream.onClose(knownCode ? (data.code as StreamCloseCode) : StreamCloseCode.Error);
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
        this.streamQueue.forEach((requests) => {
            requests.forEach((request) => {
                this.writeClose(request.id, StreamCloseCode.Lost);
            });
        });
        this.streamQueue.clear();
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

/**
 * Callbacks and limits supplied by whoever owns a {@link Connection}. `Client.connect()` builds one internally (only `hp` is
 * configurable there, through `ClientOptions.hp`); servers build their own.
 *
 * All durations are counted in ticks of the connection's 1-second timer. Use positive integers: the countdowns end when they
 * reach exactly 0.
 *
 * @example
 * ```ts
 * import { Connection, ConnectionInterface } from '@araminix/lib';
 *
 * const inter: ConnectionInterface = {
 *     hp: 120,
 *     streamRequestHp: 15,
 *     maxPendingStreams: 64,
 *     onOpen: () => console.log('socket open'),
 *     onAccept: () => console.log('endpoint accepted'),
 *     onDisconnect: (code) => console.log('socket closed with', code),
 *     onDestroy: () => console.log('connection destroyed'),
 * };
 * const connection = new Connection(inter, '');
 * ```
 */
export interface ConnectionInterface {
    /**
     * Ticks (seconds) the connection may spend without an attached socket before it is destroyed with {@link CloseCodes.Died}.
     * Only counts down while no socket is attached, and is refilled when a socket opens (or is attached with `reset`).
     *
     * @defaultValue 300 (5 minutes)
     */
    hp?: number;
    /**
     * Ticks an incoming `new_stream` request waits for {@link Connection.acquireStream} before it is dropped and the peer gets
     * `close_stream` with `Lost`.
     *
     * @defaultValue 10
     */
    streamRequestHp?: number;
    /**
     * Maximum number of queued (not yet acquired) incoming stream requests, across all names. Further `new_stream` requests are
     * answered with `close_stream` `Error`.
     *
     * @defaultValue `undefined` (no limit)
     */
    maxPendingStreams?: number;
    /** Called once, at the end of {@link Connection.destroy}. */
    onDestroy: () => void;
    /**
     * Called when the attached socket closes, with its WebSocket close code (e.g. 1006, or one of {@link WsCloseCodes}). The
     * connection stays alive; the owner decides whether to attach a new socket or destroy it.
     */
    onDisconnect: (code: number) => void;
    /** Called when the first `accept_endpoint` frame arrives (client side). Later ones are ignored. */
    onAccept: () => void;
    /** Optional: called whenever the attached socket fires `open`. */
    onOpen?: () => void;
}

/**
 * WebSocket close codes with a meaning in this protocol. 4000-4999 are application codes in RFC 6455.
 *
 * `Client.connect()` treats 4401, 4403, 4404, 4409 and 4000 as final (see `ConnectFail`) and reconnects with backoff after any
 * other code, including 1006 (abnormal closure) and 1011.
 *
 * @example
 * ```ts
 * // Server side: refuse an unauthenticated upgrade.
 * ws.close(WsCloseCodes.Unauthorized);
 * ```
 */
export enum WsCloseCodes {
    /** 1011, the RFC 6455 "internal error" code: the server failed while handling the connection. Clients reconnect with backoff. */
    InternalError = 1011,
    /** 4404: the endpoint does not exist. Client result: `ConnectFail.NotFound`. */
    NotFound = 4404,
    /**
     * 4409: another connection for the same client, endpoint and arg replaced this one. Also sent by {@link Connection.connect} to
     * the socket it replaces. Client result: `ConnectFail.Concurrent`.
     */
    ConcurrentConnection = 4409,
    /** 4401: not authenticated (missing or invalid session cookie or token). Client result: `ConnectFail.Unauthorized`. */
    Unauthorized = 4401,
    /** 4403: the server refused the endpoint ({@link Connection.deny}) or the request origin. Client result: `ConnectFail.Denied`. */
    Denied = 4403,
    /** 4000: deliberate close, sent when a connection is destroyed. The client stops (`ConnectFail.Unexpected` if still connecting). */
    Normal = 4000,
}

/**
 * Why a {@link Connection} was destroyed: the value of {@link Connection.closePromise} and `Socket.waitClose()`.
 *
 * The descriptions say which situations produce each code on the client (`Client.connect()`); servers choose their own codes
 * when they call {@link Connection.destroy}.
 *
 * @example
 * ```ts
 * const reason = await socket.waitClose();
 * if (reason === CloseCodes.Unauthorized) {
 *     console.warn('the server no longer accepts this session; log in again');
 * }
 * ```
 */
export enum CloseCodes {
    /** No socket was attached for {@link ConnectionInterface.hp} ticks. */
    Died = 'Died',
    /**
     * Something went wrong: the connect timeout expired, the reconnect limit was reached, the server answered 4404, no
     * WebSocket implementation exists, or `accept()` could not be sent.
     */
    Unexpected = 'Unexpected',
    /** Closed on purpose: `Socket.destroy()` / `AgentClient.destroy()`, or the server closed with 4000. */
    Intentional = 'Intentional',
    /** The server reported that another connection took over (4409). */
    Concurrent = 'Concurrent',
    /** Authentication or authorisation failed: close code 4401 or 4403, or {@link Connection.deny}. */
    Unauthorized = 'Unauthorized',
}
