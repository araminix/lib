import { CloseCodes, Connection, MinimalWebSocket, WsCloseCodes } from '../connection/index.js';
import { Socket } from '../socket/index.js';
import { logger } from '../utils/logger/index.js';
import { deal, Err, Ok, Result } from '../utils/result.js';
import { ConnectFail } from './types.js';

/** Milliseconds a reopened socket must stay open before the reconnect backoff resets. */
const RECONNECT_STABLE_MS = 5000;

/** Constructor signature of the global `WebSocket`; Node's built-in implementation also accepts `{ headers }`. */
type WebSocketConstructor = new (url: string, init?: { headers: Record<string, string> }) => MinimalWebSocket;

/** Parameters needed to open and maintain a client endpoint connection. */
export interface EndpointConnectorConfig {
    url: string;
    arg: string;
    token?: string;
    hp?: number;
    connectTimeout?: number;
    maxReconnectAttempts?: number;
}

/** Terminal close codes that stop reconnecting immediately, mapped to their `connect()` error and connection close code. */
const TERMINAL_CLOSE_CODES: Readonly<Record<number, readonly [ConnectFail, CloseCodes]>> = {
    [WsCloseCodes.ConcurrentConnection]: [ConnectFail.Concurrent, CloseCodes.Concurrent],
    [WsCloseCodes.Denied]: [ConnectFail.Denied, CloseCodes.Unauthorized],
    [WsCloseCodes.Normal]: [ConnectFail.Unexpected, CloseCodes.Intentional],
    [WsCloseCodes.Unauthorized]: [ConnectFail.Unauthorized, CloseCodes.Unauthorized],
    [WsCloseCodes.NotFound]: [ConnectFail.NotFound, CloseCodes.Unexpected],
};

/**
 * Opens a socket with the global `WebSocket`, passing `headers` only in Node.js (browsers cannot set them).
 * Throws when no global `WebSocket` exists.
 */
function createWebSocket(url: string, headers: Record<string, string> | undefined): MinimalWebSocket {
    const NativeWebSocket = (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
    if (!NativeWebSocket) {
        throw new Error('No global WebSocket available: @araminix/lib needs a browser or Node.js >= 22');
    }
    // Node's built-in WebSocket accepts { headers }; browsers cannot send custom headers and rely on the session cookie.
    const isNode = typeof process !== 'undefined' && Boolean(process.versions?.node);
    return headers && isNode ? new NativeWebSocket(url, { headers }) : new NativeWebSocket(url);
}

/** Computes exponential backoff delay with jitter: `min(30 s, 500 ms * 2^n) * random(0.5..1.5)`. */
function calculateReconnectDelay(reconnectAttempts: number): number {
    const baseDelay = Math.min(30000, 500 * Math.pow(2, Math.min(reconnectAttempts, 20)));
    const jitter = 0.5 + Math.random();
    return Math.round(baseDelay * jitter);
}

/**
 * Opens a new WebSocket for `url` and attaches it to `connection` without refilling hp (that happens when it opens). Does
 * nothing if the connection is dead; destroys it with `Unexpected` if no WebSocket can be created.
 */
function openAndAttachSocket(url: string, token: string | undefined, connection: Connection): void {
    if (connection.dead) {
        return;
    }
    const headers = token ? { 'x-token': token } : undefined;
    let ws: MinimalWebSocket;
    try {
        ws = createWebSocket(url, headers);
    } catch (error) {
        logger.error('Client - cannot open WebSocket:', error);
        connection.destroy(CloseCodes.Unexpected);
        return;
    }
    connection.connect(ws, false);
}

/**
 * Establishes and maintains a resilient WebSocket endpoint connection with connect timeout, close-code handling,
 * exponential backoff with jitter, and stability-based backoff reset.
 */
export function connectEndpoint({ url, arg, token, hp, connectTimeout, maxReconnectAttempts }: EndpointConnectorConfig): Promise<Result<Socket, ConnectFail>> {
    let connection!: Connection;
    const [promise, resolve] = deal<Result<Socket, ConnectFail>>();
    let reconnectAttempts = 0;
    let reconnectTimer: NodeJS.Timeout | undefined;
    let connectTimer: NodeJS.Timeout | undefined;
    let stableTimer: NodeJS.Timeout | undefined;

    const clearStableTimer = () => {
        if (stableTimer) {
            clearTimeout(stableTimer);
            stableTimer = undefined;
        }
    };

    const clearTimers = () => {
        if (connectTimer) {
            clearTimeout(connectTimer);
            connectTimer = undefined;
        }
        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = undefined;
        }
        clearStableTimer();
    };

    const failAndDestroy = (failReason: ConnectFail, closeCode: CloseCodes) => {
        clearTimers();
        resolve(new Err(failReason));
        connection.destroy(closeCode);
    };

    connection = new Connection(
        {
            hp,
            onDestroy: () => {
                clearTimers();
                resolve(new Err(ConnectFail.Unexpected));
            },
            onOpen: () => {
                clearStableTimer();
                stableTimer = setTimeout(() => {
                    stableTimer = undefined;
                    reconnectAttempts = 0;
                }, RECONNECT_STABLE_MS);
                if (typeof stableTimer.unref === 'function') {
                    stableTimer.unref();
                }
            },
            onDisconnect: (code: number) => {
                clearStableTimer();
                if (connection.dead) {
                    // A close that follows destroy() (e.g. after connectTimeout) must not schedule a reconnect.
                    return;
                }
                const terminal = TERMINAL_CLOSE_CODES[code];
                if (terminal) {
                    failAndDestroy(terminal[0], terminal[1]);
                    return;
                }
                if (maxReconnectAttempts !== undefined && reconnectAttempts >= maxReconnectAttempts) {
                    failAndDestroy(ConnectFail.Unexpected, CloseCodes.Unexpected);
                    return;
                }
                const delay = calculateReconnectDelay(reconnectAttempts);
                reconnectAttempts++;
                reconnectTimer = setTimeout(() => {
                    reconnectTimer = undefined;
                    openAndAttachSocket(url, token, connection);
                }, delay);
            },
            onAccept: () => {
                clearTimers();
                reconnectAttempts = 0;
                resolve(new Ok(new Socket(connection)));
            },
        },
        arg,
    );

    if (connectTimeout !== undefined && connectTimeout >= 0) {
        connectTimer = setTimeout(() => {
            connectTimer = undefined;
            resolve(new Err(ConnectFail.Unexpected));
            connection.destroy(CloseCodes.Unexpected);
        }, connectTimeout);
    }

    openAndAttachSocket(url, token, connection);
    return promise;
}
