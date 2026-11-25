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
