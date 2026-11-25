import { DEFAULT_STREAM_REQUEST_HP } from './types.js';

/** Payload of an incoming `new_stream` request. */
export interface PendingStreamMessage {
    id: string;
    name: string;
    args: unknown;
}

/** An incoming `new_stream` request waiting for {@link Connection.acquireStream}; it loses 1 hp per tick and is cancelled at 0. */
export class StreamRequest {
    /** The validated `new_stream` message. */
    message: PendingStreamMessage;
    /** Called when hp reaches 0; removes the request from the queue and closes it with `Lost`. */
    onCancel: () => void;
    /** Ticks left before the request is cancelled. */
    hp: number;

    /** Creates a request; `hp` defaults to {@link DEFAULT_STREAM_REQUEST_HP}. */
    constructor(message: PendingStreamMessage, onCancel: () => void, hp: number = DEFAULT_STREAM_REQUEST_HP) {
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
 * FIFO queue of incoming `new_stream` requests keyed by stream name, with per-request TTL countdowns.
 */
export class StreamRequestQueue {
    /** Incoming `new_stream` requests that were not acquired yet, by stream name (FIFO per name). */
    private readonly queue: Map<string, StreamRequest[]> = new Map();

    /** Adds a request to the tail of its stream name queue; invokes `onExpire` if its hp reaches 0 before acquisition. */
    enqueue(message: PendingStreamMessage, onExpire: (id: string) => void, hp?: number): void {
        const streamRequest = new StreamRequest(
            message,
            () => {
                const requests = this.queue.get(message.name);
                if (!requests) {
                    return;
                }
                const remaining = requests.filter((request) => request.id !== message.id);
                if (remaining.length === requests.length) {
                    return;
                }
                onExpire(message.id);
                if (remaining.length === 0) {
                    this.queue.delete(message.name);
                    return;
                }
                this.queue.set(message.name, remaining);
            },
            hp,
        );
        const list = this.queue.get(message.name);
        if (list) {
            list.push(streamRequest);
        } else {
            this.queue.set(message.name, [streamRequest]);
        }
    }

    /** Pops the oldest queued request for `name`, if any. */
    shift(name: string): StreamRequest | undefined {
        return this.queue.get(name)?.shift();
    }

    /** Puts a request back at the head of its stream name queue (e.g. when `accept_stream` could not be sent). */
    unshift(name: string, request: StreamRequest): void {
        const list = this.queue.get(name);
        if (list) {
            list.unshift(request);
        } else {
            this.queue.set(name, [request]);
        }
    }

    /** Whether a request with this stream id is waiting in the queue. */
    isQueued(id: string): boolean {
        for (const requests of this.queue.values()) {
            if (requests.some((request) => request.id === id)) {
                return true;
            }
        }
        return false;
    }

    /** Number of queued (not yet acquired) stream requests across all names. */
    count(): number {
        let total = 0;
        this.queue.forEach((requests) => {
            total += requests.length;
        });
        return total;
    }

    /** Advances every queued request by one tick, expiring those whose hp reaches 0. */
    tick(): void {
        this.queue.forEach((requests) => {
            requests.forEach((request) => request.damage());
        });
    }

    /** Invokes `onDrop` for every queued request id and empties the queue. */
    drainAndClear(onDrop: (id: string) => void): void {
        this.queue.forEach((requests) => {
            requests.forEach((request) => onDrop(request.id));
        });
        this.queue.clear();
    }
}
