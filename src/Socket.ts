import { CloseCodes, Connection } from './connection.js';
import { InitStreamErr, Stream } from './stream.js';
import { Result } from './utils/result.js';

/**
 * Why {@link Socket.incomingStream} (or `Connection.acquireStream()`, `AgentClient.onTask()`) did not deliver a stream.
 *
 * @remarks
 * Unlike most enums of this package its members are numbers.
 *
 * @example
 * ```ts
 * const incoming = await socket.incomingStream('jobs');
 * if (incoming.isErr() && incoming.e === SocketErr.Concurrent) {
 *     console.warn('another incomingStream("jobs") call took over');
 * }
 * ```
 */
export enum SocketErr {
    /** The connection is dead: it was destroyed before or while waiting. */
    Closed = 0,
    /** A newer call for the same stream name replaced this one. Only one caller can wait per name; the newest wins. */
    Concurrent = 1,
}

/**
 * State of the WebSocket currently attached to a connection, as returned by {@link Socket.getConnectionStatus}. Mirrors the
 * WHATWG `WebSocket.readyState` values, plus {@link ConnectionStatus.Unknown}.
 *
 * @example
 * ```ts
 * const online = socket.getConnectionStatus() === ConnectionStatus.Open;
 * ```
 */
export enum ConnectionStatus {
    /** `readyState` 0: the socket is opening (e.g. during a reconnect attempt). */
    Connecting = 'connecting',
    /** `readyState` 1: the socket is open and frames can be sent. */
    Open = 'open',
    /** `readyState` 2: the closing handshake is in progress. */
    Closing = 'closing',
    /**
     * `readyState` 3: the socket is closed. Usually short-lived: once the close event has been handled the socket is detached and
     * the status becomes {@link ConnectionStatus.Unknown}.
     */
    Closed = 'closed',
    /** No socket is attached (for example while waiting to reconnect), or its `readyState` is missing or unrecognised. */
    Unknown = 'unknown',
}

/**
 * The application's handle on one accepted endpoint connection.
 *
 * A `Socket` wraps a {@link Connection} and exposes the multiplexing API: open streams towards the peer with
 * {@link Socket.outgoingStream}, accept streams the peer opens with {@link Socket.incomingStream}, observe the lifecycle with
 * {@link Socket.waitClose} and {@link Socket.isDead}, and end everything with {@link Socket.destroy}.
 *
 * Clients get a `Socket` from `Client.connect()` once the server has accepted the endpoint; servers (such as
 * `@araminix/platform`) wrap their own connections. A socket survives network drops: the connection reconnects underneath it and
 * its streams continue on the new WebSocket.
 *
 * @example
 * ```ts
 * import { Client, LoopResult, loop } from '@araminix/lib';
 *
 * const client = new Client({ host: 'https://api.example.com', wsHost: 'wss://api.example.com' });
 * const connected = await client.connect('chat', 'room-42');
 * if (connected.isErr()) throw new Error(`connect failed: ${connected.e}`);
 * const socket = connected.unwrap();
 *
 * // Answer every "echo" stream the server opens.
 * loop(async () => {
 *     const incoming = await socket.incomingStream<void, string, string>('echo');
 *     if (incoming.isErr()) return LoopResult.Stop;
 *     const stream = incoming.unwrap();
 *     const message = await stream.read();
 *     if (message.isOk()) await stream.write(message.unwrap());
 *     stream.closeOk();
 *     return LoopResult.Cont;
 * });
 *
 * console.log('socket closed:', await socket.waitClose());
 * ```
 */
export class Socket {
    /** The underlying {@link Connection}. Exposed for advanced use such as servers and diagnostics; prefer the socket methods. */
    connection: Connection;

    /**
     * Wraps a connection. Clients normally get sockets from `Client.connect()` instead.
     *
     * @param connection - The connection whose streams the socket exposes.
     *
     * @example
     * ```ts
     * // Server side, after accepting the endpoint:
     * if (connection.accept().isOk()) {
     *     const socket = new Socket(connection);
     * }
     * ```
     */
    constructor(connection: Connection) {
        this.connection = connection;
    }

    /**
     * Waits for the peer to open a stream called `name` and returns it once accepted.
     *
     * A request that is already queued is accepted as soon as `accept_stream` can be sent; otherwise the call waits for the next
     * one. Only one caller can wait per name: a second call resolves the first one with {@link SocketErr.Concurrent}. Requests
     * that nobody acquires are dropped (the peer gets `Lost`) after `ConnectionInterface.streamRequestHp` ticks, 10 seconds by
     * default, so serve a name continuously, typically with `loop()`.
     *
     * @typeParam TArgs - Type of the stream arguments ({@link Stream.args}). Not validated at runtime.
     * @typeParam TDataIn - Type of the messages you read.
     * @typeParam TDataOut - Type of the messages you write.
     * @param name - The stream name the peer passes to its `outgoingStream()`.
     * @returns `Ok(stream)`; `Err(SocketErr.Closed)` if the connection is or becomes dead; `Err(SocketErr.Concurrent)` if a newer
     * call for the same name replaced this one. Never rejects.
     *
     * @example
     * ```ts
     * loop(async () => {
     *     const incoming = await socket.incomingStream<{ sensorId: string }, number>('telemetry');
     *     if (incoming.isErr()) return LoopResult.Stop;
     *     const stream = incoming.unwrap();
     *     const reading = await stream.read();
     *     if (reading.isOk()) console.log(stream.args.sensorId, reading.unwrap());
     *     stream.closeOk();
     *     return LoopResult.Cont;
     * });
     * ```
     */
    async incomingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>> {
        return await this.connection.acquireStream<TArgs, TDataIn, TDataOut>(name);
    }

    /** The endpoint argument: the `arg` passed to `Client.connect(name, arg)`, or `''`. */
    get arg(): string {
        return this.connection.arg;
    }

    /**
     * Resolves once the connection is destroyed, with the reason.
     *
     * @returns A promise of the {@link CloseCodes} reason; never rejects. For example `Intentional` after {@link Socket.destroy} or
     * when the server closed the connection with 4000, `Died` after `hp` ticks without a socket, `Concurrent` when the server
     * reported a takeover (4409), or `Unauthorized` when a reconnect was refused with 4401/4403. Resolves immediately when the
     * socket is already dead.
     *
     * @example
     * ```ts
     * once(async () => {
     *     const reason = await socket.waitClose();
     *     if (reason !== CloseCodes.Intentional) {
     *         console.warn('connection lost:', reason); // create a new one with client.connect() if needed
     *     }
     * });
     * ```
     */
    async waitClose(): Promise<CloseCodes> {
        return this.connection.closePromise;
    }

    /**
     * Whether the connection has been destroyed. A dead socket never recovers; connect again to get a new one.
     *
     * @returns `true` once destroyed for any reason; `false` otherwise, including while reconnecting.
     */
    isDead(): boolean {
        return this.connection?.dead === true;
    }

    /**
     * Closes the connection for good with {@link CloseCodes.Intentional}.
     *
     * Every stream is closed with `Lost`, waiting {@link Socket.incomingStream} calls resolve with {@link SocketErr.Closed}, the
     * WebSocket is closed with 4000 and the client stops reconnecting. Calling it again has no effect.
     *
     * @example
     * ```ts
     * window.addEventListener('pagehide', () => socket.destroy());
     * ```
     */
    destroy(): void {
        this.connection.destroy(CloseCodes.Intentional);
    }

    /**
     * Opens a new stream called `name` towards the peer.
     *
     * Sends `new_stream` with `args` and resolves when the peer accepts it. If the socket cannot send yet (e.g. during a
     * reconnect) the request is retried on every tick. The peer has to accept within 20 ticks, and must acquire the name before its
     * own `streamRequestHp` (10 ticks by default) runs out; otherwise the call resolves with {@link InitStreamErr.Dead}.
     *
     * @typeParam TArgs - Type of `args`.
     * @typeParam TDataIn - Type of the messages the peer sends back.
     * @typeParam TDataOut - Type of the messages you write.
     * @param name - The stream name; the peer accepts it with `incomingStream(name)`.
     * @param args - Optional JSON-serialisable arguments, delivered to the peer as {@link Stream.args}.
     * @returns `Ok(stream)` once accepted; `Err(InitStreamErr.Dead)` if the connection is dead, the peer rejected the stream or did
     * not accept it in time. Never rejects.
     *
     * @example
     * ```ts
     * const opened = await socket.outgoingStream<{ room: string }, string, string>('chat', { room: 'general' });
     * if (opened.isOk()) {
     *     const stream = opened.unwrap();
     *     await stream.write('hello');
     *     const reply = await stream.read(10);
     *     if (reply.isOk()) console.log('reply:', reply.unwrap());
     *     stream.closeOk();
     * }
     * ```
     */
    outgoingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string, args?: TArgs): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>> {
        return this.connection.newOutgoingStream<TArgs, TDataIn, TDataOut>(name, args as TArgs);
    }

    /**
     * Reports the state of the WebSocket currently attached to the connection.
     *
     * @returns The {@link ConnectionStatus} for `ws.readyState`, or {@link ConnectionStatus.Unknown} when no socket is attached
     * (for example between reconnect attempts) or the state cannot be read. Never throws.
     *
     * @example
     * ```ts
     * setInterval(() => {
     *     statusLabel.textContent = socket.getConnectionStatus(); // 'open', 'connecting', 'unknown', ...
     * }, 1000);
     * ```
     */
    getConnectionStatus(): ConnectionStatus {
        try {
            const ws = this.connection?.ws;
            if (!ws) return ConnectionStatus.Unknown;
            switch (ws.readyState) {
                case 0:
                    return ConnectionStatus.Connecting;
                case 1:
                    return ConnectionStatus.Open;
                case 2:
                    return ConnectionStatus.Closing;
                case 3:
                    return ConnectionStatus.Closed;
                default:
                    return ConnectionStatus.Unknown;
            }
        } catch {
            return ConnectionStatus.Unknown;
        }
    }
}
