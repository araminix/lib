import axios, { AxiosResponse } from 'axios';

import { CloseCodes, Connection, MinimalWebSocket, WsCloseCodes } from './connection.js';
import { Socket, SocketErr } from './Socket.js';
import { err, logger, ok, once, R, trimSlash, wrap } from './utils/index.js';
import { deal, Err, Ok, Result } from './utils/result.js';

export const axiosService = axios;

const RECONNECT_STABLE_MS = 5000; // a reopened socket must stay up this long before the backoff resets

type WebSocketConstructor = new (url: string, init?: { headers: Record<string, string> }) => MinimalWebSocket;

function createWebSocket(url: string, headers: Record<string, string> | undefined): MinimalWebSocket {
    const NativeWebSocket = (globalThis as { WebSocket?: WebSocketConstructor }).WebSocket;
    if (!NativeWebSocket) {
        throw new Error('No global WebSocket available: @araminix/lib needs a browser or Node.js >= 22');
    }
    // Node's built-in WebSocket accepts { headers }; browsers cannot send custom headers and rely on the session cookie.
    const isNode = typeof process !== 'undefined' && Boolean(process.versions?.node);
    return headers && isNode ? new NativeWebSocket(url, { headers }) : new NativeWebSocket(url);
}

export interface FileInfo {
    name: string;
    size: number;
    mimetype: string;
    id?: string;
    userId: string;
}

export interface ClientOptions {
    hp?: number;
    host: string;
    wsHost: string;
    mount?: string;
    token?: string;
    connectTimeout?: number;
    maxReconnectAttempts?: number;
}

export enum ConnectFail {
    Unexpected = 'Unexpected',
    Unauthorized = 'Unauthorized',
    NotFound = 'NotFound',
    Denied = 'Denied',
}

export enum AgentClientErrorType {
    Communication = 'Communication',
    Execution = 'Execution',
}

export abstract class AgentClientError {
    abstract type: AgentClientErrorType;
}

export class CommunicationClientAgentError<E = unknown> implements AgentClientError {
    readonly e: E;
    readonly type = AgentClientErrorType.Communication;

    constructor(e: E) {
        this.e = e;
    }
}

export class ExecutionClientAgentError<E = unknown> implements AgentClientError {
    readonly e: E;
    readonly type = AgentClientErrorType.Execution;

    constructor(e: E) {
        this.e = e;
    }
}

export class AgentClient {
    private socket: Socket;

    constructor(socket: Socket) {
        this.socket = socket;
    }

    async onTask<TTask = unknown, TResult = unknown, TError = unknown>(name: string): Promise<Result<[TTask, (value: Result<TResult, TError>) => R], SocketErr>> {
        const streamResult = await this.socket.incomingStream<TTask, { ok: boolean; v?: TResult; e?: TError }>(name);
        if (streamResult.isErr()) {
            return new Err(streamResult.e);
        }
        const stream = streamResult.unwrap();
        const [promise, resolve] = deal<Result<TResult, TError>>();
        once(async () => {
            const result = await promise;
            await stream.write(result.serialize());
        });
        return new Ok([stream.args as TTask, resolve]);
    }

    destroy(): void {
        this.socket.destroy();
    }

    async post<TData = unknown, TResult = unknown, E = unknown>(name: string, data: TData, hp: number = 10): Promise<Result<TResult, AgentClientError>> {
        const stream = await this.socket.outgoingStream<{ name: string; data: TData }, { ok: boolean; v: TResult; e: E }>('post', { name, data });
        if (stream.isErr()) {
            return new Err(new CommunicationClientAgentError(stream.e));
        }
        const readResult = await stream.unwrap().read<{ ok: boolean; v: TResult; e: E }>(hp);
        if (readResult.isErr()) {
            return new Err(new CommunicationClientAgentError(readResult.e));
        }
        const read = readResult.unwrap();
        if (typeof read !== 'object' || read === null || !('ok' in read)) {
            return new Err(new CommunicationClientAgentError('Format'));
        }
        if (read.ok === true) {
            return new Ok(read.v);
        }
        return new Err(new ExecutionClientAgentError(read.e));
    }

    async waitClose(): Promise<CloseCodes> {
        return await this.socket.waitClose();
    }
}

export class Client {
    private readonly mount: string;
    private readonly token?: string;
    private readonly host: string;
    private readonly wsHost: string;
    private readonly hp?: number;
    private readonly connectTimeout?: number;
    private readonly maxReconnectAttempts?: number;

    constructor({ host, wsHost, mount, token, hp, connectTimeout, maxReconnectAttempts }: ClientOptions) {
        this.mount = mount ? `/${trimSlash(mount)}` : '';
        this.token = token;
        this.host = host;
        this.wsHost = wsHost;
        this.hp = hp;
        this.connectTimeout = connectTimeout;
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    private requestHeaders(): Record<string, string> {
        // The server rejects cookie-authenticated mutations without JSON or this header (CSRF guard).
        const headers: Record<string, string> = { 'X-Requested-With': 'XMLHttpRequest' };
        if (this.token) {
            headers['x-token'] = this.token;
        }
        return headers;
    }

    async auth<TAuthData = unknown, TAuthResponse = unknown, TErr = unknown>(data: TAuthData): Promise<Result<AxiosResponse<TAuthResponse>, TErr>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<TAuthResponse>, TErr>(axios.post(`${this.host}${this.mount}/auth`, data, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async gauth<TResponse = unknown, TErr = unknown>(access_token: string): Promise<Result<AxiosResponse<TResponse>, TErr | void>> {
        if (!access_token) return new Err<void, AxiosResponse<TResponse>>(undefined);
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<TResponse>, TErr>(axios.post(`${this.host}${this.mount}/gauth`, { access_token }, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async deauth(): Promise<Ok<void> | Err<void, void>> {
        const headers = this.requestHeaders();
        const res = await wrap(axios.delete(`${this.host}${this.mount}/auth`, { headers, withCredentials: true }));
        if (res.isOk()) return ok();
        return err();
    }

    async request2FA<T = unknown, E = unknown>(): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/request`, undefined, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async enable2FA<T = unknown, E = unknown>(secret: string, code: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/enable`, { secret, code }, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async verify2FA<T = unknown, E = unknown>(accountName: string, code: string, uuid: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/verify`, { accountName, code, uuid }, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async disable2FA<T = unknown, E = unknown>(code?: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(
            axios.delete(`${this.host}${this.mount}/2fa/disable`, {
                headers,
                withCredentials: true,
                data: code !== undefined ? { code } : undefined,
            }),
        );
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async upload(file: string | Blob): Promise<Result<FileInfo, void>> {
        const form = new FormData();
        form.append('file', file);
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<FileInfo>>(axios.post(`${this.host}${this.mount}/file`, form, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v.data);
        return new Err<void, FileInfo>(undefined);
    }

    getFileLink(file: string | FileInfo): string {
        if (typeof file === 'string') {
            return `${this.host}${this.mount}/file/${file}`;
        }
        return `${this.host}${this.mount}/file/${file.id}/${encodeURIComponent(file.name)}`;
    }

    async getPresignedUploadUrl<T = unknown, E = unknown>(mimetype?: string, fileName?: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/attachments/upload`, { mimetype, fileName }, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async getPresignedDownloadUrl<T = unknown, E = unknown>(fileId: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.get(`${this.host}${this.mount}/attachments/${encodeURIComponent(fileId)}`, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    private reconnect(url: string, connection: Connection): void {
        if (connection.dead) {
            return;
        }
        const headers = this.token ? { 'x-token': this.token } : undefined;
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

    async connectAgent(): Promise<Result<AgentClient, ConnectFail>> {
        const socket = await this.connect('agent');
        if (socket.isErr()) {
            return new Err(socket.e);
        }
        return new Ok(new AgentClient(socket.unwrap()));
    }

    connect(name: string, arg?: string): Promise<Result<Socket, ConnectFail>> {
        const pathArg = arg ? `/${arg}` : '';
        const url = `${this.wsHost}${this.mount}/ws/${name}${pathArg}`;
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

        const inter = {
            hp: this.hp,
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
                switch (code) {
                    case WsCloseCodes.ConcurrentConnection: {
                        clearTimers();
                        resolve(new Err(ConnectFail.Unexpected));
                        connection.destroy(CloseCodes.Concurrent);
                        break;
                    }
                    case WsCloseCodes.Denied: {
                        clearTimers();
                        resolve(new Err(ConnectFail.Denied));
                        connection.destroy(CloseCodes.Unauthorized);
                        break;
                    }
                    case WsCloseCodes.Normal: {
                        clearTimers();
                        resolve(new Err(ConnectFail.Unexpected));
                        connection.destroy(CloseCodes.Intentional);
                        break;
                    }
                    case WsCloseCodes.Unauthorized: {
                        clearTimers();
                        resolve(new Err(ConnectFail.Unauthorized));
                        connection.destroy(CloseCodes.Unauthorized);
                        break;
                    }
                    case WsCloseCodes.NotFound: {
                        clearTimers();
                        resolve(new Err(ConnectFail.NotFound));
                        connection.destroy(CloseCodes.Unexpected);
                        break;
                    }
                    default: {
                        if (this.maxReconnectAttempts !== undefined && reconnectAttempts >= this.maxReconnectAttempts) {
                            clearTimers();
                            resolve(new Err(ConnectFail.Unexpected));
                            connection.destroy(CloseCodes.Unexpected);
                            break;
                        }
                        const baseDelay = Math.min(30000, 500 * Math.pow(2, Math.min(reconnectAttempts, 20)));
                        const jitter = 0.5 + Math.random();
                        const delay = Math.round(baseDelay * jitter);
                        reconnectAttempts++;
                        reconnectTimer = setTimeout(() => {
                            reconnectTimer = undefined;
                            this.reconnect(url, connection);
                        }, delay);
                        break;
                    }
                }
            },
            onAccept: () => {
                clearTimers();
                reconnectAttempts = 0;
                resolve(new Ok(new Socket(connection)));
            },
        };
        connection = new Connection(inter, arg ?? '');
        if (this.connectTimeout !== undefined && this.connectTimeout >= 0) {
            connectTimer = setTimeout(() => {
                connectTimer = undefined;
                resolve(new Err(ConnectFail.Unexpected));
                connection.destroy(CloseCodes.Unexpected);
            }, this.connectTimeout);
        }
        this.reconnect(url, connection);
        return promise;
    }
}
