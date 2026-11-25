import { logger } from '../utils/logger/index.js';
import { MinimalWebSocket } from './types.js';

let fallbackTextEncoder: TextEncoder | undefined;

/** Returns the UTF-8 byte length of `value`, using `Buffer.byteLength` in Node.js with a `TextEncoder` fallback for browsers. */
export function utf8ByteLength(value: string): number {
    if (typeof Buffer !== 'undefined' && typeof Buffer.byteLength === 'function') {
        return Buffer.byteLength(value, 'utf8');
    }
    fallbackTextEncoder ??= new TextEncoder();
    return fallbackTextEncoder.encode(value).byteLength;
}

/**
 * Tracks WebSocket send-buffer backpressure across ticks and terminates sockets that stay stalled beyond the configured limit.
 */
export class BackpressureMonitor {
    /** `true` once a write was dropped due to backpressure since the last successful send, pong, drain, or socket attach. */
    private active: boolean = false;
    /** Consecutive 1-second ticks during which the attached socket remained backpressured. */
    private ticks: number = 0;
    /** Smallest frame byte length rejected during the current backpressure episode, used to detect when the buffer drains. */
    private lastBlockedPayloadBytes: number = 0;

    /**
     * Checks whether sending `payload` would exceed `maxBufferedBytes`. If so, records the drop and returns `true`.
     */
    wouldOverflow(ws: MinimalWebSocket, payload: string, maxBufferedBytes: number): boolean {
        if (typeof ws.bufferedAmount !== 'number' || maxBufferedBytes <= 0) {
            return false;
        }
        const payloadBytes = utf8ByteLength(payload);
        if (ws.bufferedAmount + payloadBytes > maxBufferedBytes) {
            this.onDrop(payloadBytes);
            return true;
        }
        return false;
    }

    /** Records a write rejected due to backpressure so {@link BackpressureMonitor.check} can track sustained stalls. */
    private onDrop(payloadBytes: number): void {
        this.active = true;
        if (this.lastBlockedPayloadBytes === 0 || payloadBytes < this.lastBlockedPayloadBytes) {
            this.lastBlockedPayloadBytes = payloadBytes;
        }
    }

    /** Clears backpressure tracking state after a successful send, pong, drain, or socket lifecycle change. */
    reset(): void {
        this.active = false;
        this.ticks = 0;
        this.lastBlockedPayloadBytes = 0;
    }

    /** Advances or clears the sustained-backpressure counter on each tick and terminates stalled sockets past the limit. */
    check(ws: MinimalWebSocket | undefined, maxBufferedBytes: number, backpressureHp: number): void {
        if (!ws || typeof ws.bufferedAmount !== 'number' || maxBufferedBytes <= 0 || backpressureHp <= 0) {
            this.reset();
            return;
        }
        const stillBackpressured =
            ws.bufferedAmount > maxBufferedBytes || (this.active && ws.bufferedAmount > 0 && ws.bufferedAmount + this.lastBlockedPayloadBytes > maxBufferedBytes);
        if (!stillBackpressured) {
            this.reset();
            return;
        }
        this.ticks++;
        if (this.ticks >= backpressureHp) {
            this.closeStalledSocket(ws);
        }
    }

    /** Terminates (or closes) a WebSocket that remained backpressured past the configured threshold. */
    private closeStalledSocket(ws: MinimalWebSocket): void {
        logger.warn(`WS Connection - closing stalled socket after ${this.ticks} ticks of sustained backpressure (bufferedAmount=${ws.bufferedAmount})`);
        this.reset();
        try {
            if (typeof ws.terminate === 'function') {
                ws.terminate();
            } else {
                ws.close();
            }
        } catch {
            // ignore
        }
    }
}
