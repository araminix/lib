/**
 * Metadata of an uploaded file: the JSON body returned by `POST <host><mount>/file` and by {@link Client.upload}.
 *
 * @example
 * ```ts
 * const uploaded = await client.upload(file);
 * if (uploaded.isOk()) {
 *     const info: FileInfo = uploaded.unwrap();
 *     console.log(`${info.name} (${info.size} bytes) -> ${client.getFileLink(info)}`);
 * }
 * ```
 */
export interface FileInfo {
    /** Original file name, as stored by the server. */
    name: string;
    /** File size in bytes. */
    size: number;
    /** MIME type of the file. */
    mimetype: string;
    /** File id assigned by the server; {@link Client.getFileLink} needs it when given a `FileInfo`. */
    id?: string;
    /** Id of the user who uploaded the file. */
    userId: string;
}

/**
 * Options of {@link Client}. See the README's configuration reference for a summary table.
 *
 * @example
 * ```ts
 * import { Client, ClientOptions } from '@araminix/lib';
 *
 * const options: ClientOptions = {
 *     host: 'https://api.example.com',
 *     wsHost: 'wss://api.example.com',
 *     mount: '/api',
 *     connectTimeout: 10_000,
 *     maxReconnectAttempts: 8,
 * };
 * const client = new Client(options);
 * ```
 */
export interface ClientOptions {
    /**
     * Connection hp, in ticks (seconds): how long a connection may go without an open socket before it is destroyed with
     * `CloseCodes.Died`. The countdown only runs while waiting between reconnect attempts and is refilled whenever a socket opens.
     *
     * @defaultValue 300 (5 minutes)
     */
    hp?: number;
    /** Base URL for HTTP calls, without a trailing slash, e.g. `'https://api.example.com'`. */
    host: string;
    /**
     * Base URL for WebSocket connections, without a trailing slash, e.g. `'wss://api.example.com'`. Endpoints are opened at
     * `${wsHost}${mount}/ws/<name>[/<arg>]`.
     */
    wsHost: string;
    /**
     * Path prefix the server is mounted under; used for HTTP and WebSocket URLs alike. Leading and trailing slashes are
     * normalised, so `'api/v1/'` and `'/api/v1'` both become `/api/v1`.
     *
     * @defaultValue `''` (no prefix)
     */
    mount?: string;
    /**
     * Session or agent token for Node.js clients. Sent as the `x-token` header on the WebSocket upgrade and on every HTTP call,
     * never in a URL.
     *
     * @remarks
     * Leave it unset in browsers. Browsers cannot send headers on a WebSocket upgrade and authenticate with the httpOnly session
     * cookie instead, which also keeps the token away from page scripts.
     *
     * @defaultValue `undefined` (cookie authentication)
     */
    token?: string;
    /**
     * Milliseconds `connect()` may take until the endpoint is accepted, covering all reconnect attempts in between. When it
     * expires, `connect()` resolves with `ConnectFail.Unexpected` and the connection is destroyed. Negative values are ignored;
     * `0` practically always fails. Once the endpoint is accepted the timer is cleared and no longer applies.
     *
     * @defaultValue `undefined` (no timeout; a failing attempt still ends on a final close code, on `maxReconnectAttempts`, or when
     * the connection hp runs out)
     */
    connectTimeout?: number;
    /**
     * Maximum number of consecutive reconnects after transient closes (any close code other than 4000, 4401, 4403, 4404 and 4409).
     * When the limit is reached the connection is destroyed, and a pending `connect()` resolves with `ConnectFail.Unexpected`.
     * `0` disables reconnecting. The count resets once a socket has stayed open for 5 seconds, and when the endpoint is first
     * accepted.
     *
     * @defaultValue `undefined` (unlimited)
     */
    maxReconnectAttempts?: number;
}

/**
 * Why {@link Client.connect} or {@link Client.connectAgent} failed.
 *
 * @example
 * ```ts
 * const connected = await client.connect('platform');
 * if (connected.isErr()) {
 *     switch (connected.e) {
 *         case ConnectFail.Unauthorized:
 *             break; // log in first
 *         case ConnectFail.Concurrent:
 *             break; // opened somewhere else
 *         default:
 *             break; // NotFound, Denied or Unexpected
 *     }
 * }
 * ```
 */
export enum ConnectFail {
    /**
     * Anything not covered by the other members: the connect timeout expired, the reconnect limit was reached, the connection hp
     * ran out, the server closed with 4000, or no global `WebSocket` exists.
     */
    Unexpected = 'Unexpected',
    /** Close code 4401: not authenticated (missing or expired session cookie, or invalid token). */
    Unauthorized = 'Unauthorized',
    /** Close code 4404: the endpoint does not exist on the server. */
    NotFound = 'NotFound',
    /** Close code 4403: the server refused the endpoint or the request origin. */
    Denied = 'Denied',
    /** Close code 4409: another connection for the same client, endpoint and arg took over. Since 2.1.0. */
    Concurrent = 'Concurrent',
}
