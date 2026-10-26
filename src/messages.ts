import { t } from "./utils/index.js";

export const NewStream = t.type({
    type: t.literal('new_stream'),
    id: t.string,
    name: t.string,
    args: t.unknown,
});

export const AcceptEndpoint = t.type({
    type: t.literal('accept_endpoint'),
});

export const AcceptStream = t.type({
    type: t.literal('accept_stream'),
    id: t.string,
});

export const CloseStream = t.type({
    type: t.literal('close_stream'),
    id: t.string,
    code: t.string,
});

export const DataStream = t.type({
    type: t.literal('data_stream'),
    id: t.string,
    data: t.unknown,
});

export const PingStream = t.type({
    type: t.literal('ping_stream'),
    id: t.string,
});

export const PongStream = t.type({
    type: t.literal('pong_stream'),
    id: t.string,
});

export type AcceptStreamType = t.TypeOf<typeof AcceptStream>;
export type AcceptEndpointType = t.TypeOf<typeof AcceptEndpoint>;
export type NewStreamType = t.TypeOf<typeof NewStream>;
export type CloseStreamType = t.TypeOf<typeof CloseStream>;
export type DataStreamType = t.TypeOf<typeof DataStream>;
export type PingStreamType = t.TypeOf<typeof PingStream>;
export type PongStreamType = t.TypeOf<typeof PongStream>;
