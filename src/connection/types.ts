/** Default {@link ConnectionInterface.hp}: ticks a connection may spend without a socket before it dies. */
export const DEFAULT_HP = 300; // seconds (60 * 5) 5 minutes
/** Default {@link ConnectionInterface.streamRequestHp}. */
export const DEFAULT_STREAM_REQUEST_HP = 10; // seconds a new_stream request waits for an acquirer
/** Default {@link ConnectionInterface.maxBufferedBytes}: 4 MiB. */
export const DEFAULT_MAX_BUFFERED_BYTES = 4 * 1024 * 1024;
/** Default {@link ConnectionInterface.backpressureHp}: ticks of sustained backpressure before closing a stalled socket. */
export const DEFAULT_BACKPRESSURE_HP = 10;

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
    /**
     * Optional immediate socket teardown (provided by Node's `ws` package). When present, used to kill sockets stalled under
     * sustained backpressure without waiting for a closing handshake.
     */
    terminate?: () => void;
    /**
     * Bytes queued by the WebSocket implementation waiting to be flushed to the network. Optional: when missing, backpressure
     * checks are skipped.
     */
    bufferedAmount?: number;
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
    /**
     * Maximum bytes allowed in `ws.bufferedAmount` (including the next frame's UTF-8 byte length) before outgoing writes are
     * rejected with `err()` without calling `ws.send()`. Set to `0` to disable backpressure checks.
     *
     * @defaultValue 4194304 (4 MiB)
     */
    maxBufferedBytes?: number;
    /**
     * Ticks (seconds) a socket may remain continuously backpressured (without draining, sending successfully, or receiving a
     * `pong_stream`) before the stalled socket is closed/terminated so dead slow consumers do not linger. Set to `0` to disable
     * automatic termination on sustained backpressure.
     *
     * @defaultValue 10
     */
    backpressureHp?: number;
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
    /** Authentication or authorization failed: close code 4401 or 4403, or {@link Connection.deny}. */
    Unauthorized = 'Unauthorized',
}
