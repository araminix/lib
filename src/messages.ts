import { t } from './utils/index.js';

/**
 * Codec of `new_stream`: the sender opens stream `id` named `name`, with arguments `args`.
 *
 * The receiver queues the request until the application acquires the name (`Socket.incomingStream(name)`), then replies with
 * {@link AcceptStream}. It replies with {@link CloseStream} instead when the id is already in use (`Duplicate`), its pending
 * request limit is reached (`Error`) or nobody acquires the stream in time (`Lost`).
 *
 * Like every codec in this module it is an io-ts `t.type`, available from the package as `Message.NewStream`; use `.is()` for a
 * type guard or `.decode()` for detailed validation.
 *
 * @example
 * ```ts
 * import { Message } from '@araminix/lib';
 *
 * Message.NewStream.is({ type: 'new_stream', id: 's-1', name: 'chat', args: { room: 1 } }); // true
 * Message.NewStream.is({ type: 'new_stream', id: 1 }); // false
 * ```
 */
export const NewStream = t.type({
    type: t.literal('new_stream'),
    id: t.string,
    name: t.string,
    args: t.unknown,
});

/**
 * Codec of `accept_endpoint`: the server accepted the endpoint connection.
 *
 * `Client.connect()` resolves with a `Socket` when the first one arrives; later ones (sent again after a reconnect) are ignored.
 *
 * @example
 * ```ts
 * Message.AcceptEndpoint.is({ type: 'accept_endpoint' }); // true
 * ```
 */
export const AcceptEndpoint = t.type({
    type: t.literal('accept_endpoint'),
});

/**
 * Codec of `accept_stream`: the receiver of a {@link NewStream} accepted stream `id`; the opener's `outgoingStream()` call resolves.
 *
 * @example
 * ```ts
 * Message.AcceptStream.is({ type: 'accept_stream', id: 's-1' }); // true
 * ```
 */
export const AcceptStream = t.type({
    type: t.literal('accept_stream'),
    id: t.string,
});

/**
 * Codec of `close_stream`: stream `id` was closed by the sender with `code` (a `StreamCloseCodes` value such as `'Normal'`).
 * The receiver closes its side without replying. It is also the answer to frames for unknown stream ids (`'NotFound'`).
 *
 * @example
 * ```ts
 * Message.CloseStream.is({ type: 'close_stream', id: 's-1', code: 'Normal' }); // true
 * ```
 */
export const CloseStream = t.type({
    type: t.literal('close_stream'),
    id: t.string,
    code: t.string,
});

/**
 * Codec of `data_stream`: one application message `data` on stream `id`. `data` can be any JSON value; when it was `undefined`
 * the key is missing on the wire, which the codec still accepts.
 *
 * @example
 * ```ts
 * Message.DataStream.is({ type: 'data_stream', id: 's-1', data: [1, 2, 3] }); // true
 * ```
 */
export const DataStream = t.type({
    type: t.literal('data_stream'),
    id: t.string,
    data: t.unknown,
});

/**
 * Codec of `ping_stream`: heartbeat probe for stream `id`, sent by the side that accepted the stream after 10 idle ticks.
 * The receiver answers with {@link PongStream}.
 *
 * @example
 * ```ts
 * Message.PingStream.is({ type: 'ping_stream', id: 's-1' }); // true
 * ```
 */
export const PingStream = t.type({
    type: t.literal('ping_stream'),
    id: t.string,
});

/**
 * Codec of `pong_stream`: heartbeat answer to {@link PingStream} for stream `id`.
 *
 * @example
 * ```ts
 * Message.PongStream.is({ type: 'pong_stream', id: 's-1' }); // true
 * ```
 */
export const PongStream = t.type({
    type: t.literal('pong_stream'),
    id: t.string,
});

/** Decoded shape of {@link AcceptStream}: `{ type: 'accept_stream'; id: string }`. */
export type AcceptStreamType = t.TypeOf<typeof AcceptStream>;
/** Decoded shape of {@link AcceptEndpoint}: `{ type: 'accept_endpoint' }`. */
export type AcceptEndpointType = t.TypeOf<typeof AcceptEndpoint>;
/** Decoded shape of {@link NewStream}: `{ type: 'new_stream'; id: string; name: string; args: unknown }`. */
export type NewStreamType = t.TypeOf<typeof NewStream>;
/** Decoded shape of {@link CloseStream}: `{ type: 'close_stream'; id: string; code: string }`. */
export type CloseStreamType = t.TypeOf<typeof CloseStream>;
/** Decoded shape of {@link DataStream}: `{ type: 'data_stream'; id: string; data: unknown }`. */
export type DataStreamType = t.TypeOf<typeof DataStream>;
/** Decoded shape of {@link PingStream}: `{ type: 'ping_stream'; id: string }`. */
export type PingStreamType = t.TypeOf<typeof PingStream>;
/** Decoded shape of {@link PongStream}: `{ type: 'pong_stream'; id: string }`. */
export type PongStreamType = t.TypeOf<typeof PongStream>;
