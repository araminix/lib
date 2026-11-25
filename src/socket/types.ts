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
    /** The wait was cancelled via an `AbortSignal` or {@link Socket.cancelIncomingStream}. */
    Cancelled = 2,
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
    /** No socket is attached (for example while waiting to reconnect), or its `readyState` is missing or unrecognized. */
    Unknown = 'unknown',
}
