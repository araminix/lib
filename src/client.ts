import axios, { AxiosResponse } from 'axios';
import WS from 'websocket';

import { CloseCodes, Connection, WsCloseCodes } from './connection.js';
import { Socket, SocketErr } from './Socket.js';
import { err, ok, once, R, trimSlash, wrap } from './utils/index.js';
import { deal, Err, Ok, Result } from './utils/result.js';

export const axiosService = axios;

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
    private readonly token: string;
    private readonly host: string;
    private readonly wsHost: string;
    private readonly hp?: number;

    constructor({ host, wsHost, mount, token, hp }: ClientOptions) {
        this.mount = mount ? `/${trimSlash(mount)}` : '';
        this.token = token ? `ws-token/${token}` : 'ws';
        this.host = host;
        this.wsHost = wsHost;
        this.hp = hp;
    }

    async auth<TAuthData = unknown, TAuthResponse = unknown, TErr = unknown>(data: TAuthData): Promise<Result<AxiosResponse<TAuthResponse>, TErr>> {
        const res = await wrap<AxiosResponse<TAuthResponse>, TErr>(axios.post(`${this.host}${this.mount}/auth`, data));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async gauth<TResponse = unknown, TErr = unknown>(access_token: string): Promise<Result<AxiosResponse<TResponse>, TErr | void>> {
        if (!this.token) return new Err<void, AxiosResponse<TResponse>>(undefined);
        const res = await wrap<AxiosResponse<TResponse>, TErr>(axios.post(`${this.host}${this.mount}/gauth`, { access_token }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async deauth(): Promise<Ok<void> | Err<void, void>> {
        const res = await wrap(axios.delete(`${this.host}${this.mount}/auth`));
        if (res.isOk()) return ok();
        return err();
    }

    async request2FA<T = unknown, E = unknown>(): Promise<Result<AxiosResponse<T>, E>> {
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/request`));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async enable2FA<T = unknown, E = unknown>(secret: string, code: string): Promise<Result<AxiosResponse<T>, E>> {
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/enable`, { secret, code }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async verify2FA<T = unknown, E = unknown>(accountName: string, code: string, uuid: string): Promise<Result<AxiosResponse<T>, E>> {
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/verify`, { accountName, code, uuid }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async disable2FA<T = unknown, E = unknown>(): Promise<Result<AxiosResponse<T>, E>> {
        const res = await wrap<AxiosResponse<T>, E>(axios.delete(`${this.host}${this.mount}/2fa/disable`));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async upload(file: string | Blob): Promise<Result<FileInfo, void>> {
        const form = new FormData();
        form.append('file', file);
        const options = this.token ? { headers: { 'x-token': this.token } } : undefined;
        const res = await wrap<AxiosResponse<FileInfo>>(axios.post(`${this.host}${this.mount}/file`, form, options));
        if (res.isOk()) return new Ok(res.v.data);
        return new Err<void, FileInfo>(undefined);
    }

    getFileLink(file: string | FileInfo): string {
        if (typeof file === 'string') {
            return `${this.host}${this.mount}/file/${file}`;
        }
        return `${this.host}${this.mount}/file/${file.id}/${file.name}`;
    }

    async getPresignedUploadUrl<T = unknown, E = unknown>(): Promise<Result<AxiosResponse<T>, E>> {
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/storage/upload`));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    async getPresignedDownloadUrl<T = unknown, E = unknown>(fileId: string): Promise<Result<AxiosResponse<T>, E>> {
        const res = await wrap<AxiosResponse<T>, E>(axios.get(`${this.host}${this.mount}/storage/${fileId}`));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    private reconnect(url: string, connection: Connection): void {
        const ws = new (WS as any).w3cwebsocket(url);
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
        const url = `${this.wsHost}${this.mount}/${this.token}/${name}${pathArg}`;
        let connection!: Connection;
        const [promise, resolve] = deal<Result<Socket, ConnectFail>>();
        const inter = {
            hp: this.hp,
            onDestroy: function () {},
            onDisconnect: (code: number) => {
                switch (code) {
                    case WsCloseCodes.ConcurrentConnection: {
                        resolve(new Err(ConnectFail.Unexpected));
                        connection.destroy(CloseCodes.Concurrent);
                        break;
                    }
                    case WsCloseCodes.Denied: {
                        resolve(new Err(ConnectFail.Denied));
                        connection.destroy(CloseCodes.Unauthorized);
                        break;
                    }
                    case WsCloseCodes.Normal: {
                        resolve(new Err(ConnectFail.Unexpected));
                        connection.destroy(CloseCodes.Intentional);
                        break;
                    }
                    case WsCloseCodes.Unauthorized: {
                        resolve(new Err(ConnectFail.Unauthorized));
                        connection.destroy(CloseCodes.Unauthorized);
                        break;
                    }
                    case WsCloseCodes.NotFound: {
                        resolve(new Err(ConnectFail.NotFound));
                        connection.destroy(CloseCodes.Unexpected);
                        break;
                    }
                    default: {
                        setTimeout(() => {
                            this.reconnect(url, connection);
                        }, 500);
                        break;
                    }
                }
            },
            onAccept: () => {
                resolve(new Ok(new Socket(connection)));
            },
        };
        connection = new Connection(inter, arg ?? '');
        this.reconnect(url, connection);
        return promise;
    }
}
