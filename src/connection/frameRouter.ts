import { AcceptEndpoint, AcceptStream, CloseStream, DataStream, NewStream, PingStream, PongStream } from '../protocol/messages.js';
import { StreamCloseCodes } from '../stream/index.js';
import { logger } from '../utils/logger/index.js';

/** Callbacks invoked by {@link dispatchIncomingFrame} for each recognized protocol frame type. */
export interface IncomingFrameHandlers {
    onNewStream: (data: { id: string; name: string; args: unknown }) => void;
    onData: (data: { id: string; data: unknown }) => void;
    onCloseStream: (data: { id: string; code: string }) => void;
    onPingStream: (data: { id: string }) => void;
    onPongStream: (data: { id: string }) => void;
    onAcceptStream: (data: { id: string }) => void;
    onAcceptEndpoint: () => void;
}

/**
 * Parses a raw WebSocket message payload (`string` or buffer-like value) as JSON.
 * Logs and returns `{ ok: false }` on parse failure.
 */
export function parseIncomingMessage(rawData: any): { ok: true; data: unknown } | { ok: false } {
    try {
        const raw = typeof rawData === 'string' ? rawData : rawData.toString();
        const data = JSON.parse(raw);
        logger.trace('WS Connection - onMessage:', raw);
        return { ok: true, data };
    } catch (error) {
        logger.error(`WS Connection - onMessage JSON Parse Error:`, error);
        return { ok: false };
    }
}

/** Routes a parsed frame to its handler by message type; frames that match no codec are ignored. */
export function dispatchIncomingFrame(data: unknown, handlers: IncomingFrameHandlers): void {
    switch (true) {
        case NewStream.is(data): {
            handlers.onNewStream(data);
            break;
        }
        case DataStream.is(data): {
            handlers.onData(data);
            break;
        }
        case CloseStream.is(data): {
            handlers.onCloseStream(data);
            break;
        }
        case PingStream.is(data): {
            handlers.onPingStream(data);
            break;
        }
        case PongStream.is(data): {
            handlers.onPongStream(data);
            break;
        }
        case AcceptStream.is(data): {
            handlers.onAcceptStream(data);
            break;
        }
        case AcceptEndpoint.is(data): {
            handlers.onAcceptEndpoint();
            break;
        }
    }
}

/**
 * Validates a peer-supplied `close_stream` code against {@link StreamCloseCodes}, falling back to `Error` so `waitClose()`
 * never stays pending on an unknown or empty code.
 */
export function normalizeStreamCloseCode(code: string): StreamCloseCodes {
    const knownCode = (Object.values(StreamCloseCodes) as string[]).includes(code);
    return knownCode ? (code as StreamCloseCodes) : StreamCloseCodes.Error;
}
