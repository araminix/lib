import axios, { AxiosResponse } from 'axios';

import { Socket } from '../socket/index.js';
import { trimSlash, wrap } from '../utils/helpers.js';
import { err, Err, ok, Ok, Result } from '../utils/result.js';
import { AgentClient } from './AgentClient.js';
import { connectEndpoint } from './connector.js';
import { ClientOptions, ConnectFail, FileInfo } from './types.js';

/**
 * The axios default instance that {@link Client} uses for every HTTP call.
 *
 * Exposed so applications can add interceptors or defaults (timeouts, extra headers) without importing axios themselves.
 * Changes affect every `Client` in the process, and any other code that uses the axios default instance.
 *
 * @example
 * ```ts
 * import { axiosService } from '@araminix/lib';
 *
 * axiosService.defaults.timeout = 15_000;
 * axiosService.interceptors.response.use(undefined, (error) => {
 *     if (error.response?.status === 401) window.location.assign('/login');
 *     return Promise.reject(error);
 * });
 * ```
 */
export const axiosService = axios;

/**
 * Client for an `@araminix/platform` server, or any server that speaks the same protocol: HTTP helpers for authentication, 2FA
 * and files, plus multiplexed WebSocket endpoints.
 *
 * WebSocket endpoints are opened at `${wsHost}${mount}/ws/<name>[/<arg>]` with the global `WebSocket` (browsers and Node.js >= 22;
 * no extra dependency). Authentication depends on the environment:
 * - **Browsers:** leave {@link ClientOptions.token} unset. The httpOnly session cookie set by {@link Client.auth} is sent with HTTP
 *   calls (`withCredentials: true`) and with the WebSocket upgrade.
 * - **Node.js:** set `token`. It is sent as the `x-token` header on the upgrade (`new WebSocket(url, { headers })`) and on HTTP calls.
 *
 * Every HTTP call also sends `X-Requested-With: XMLHttpRequest`, which the server's CSRF guard requires for cookie-authenticated
 * requests, uses `withCredentials: true`, and resolves to a {@link Result} instead of throwing. Requests go through
 * {@link axiosService}.
 *
 * @example
 * ```ts
 * import { Client, ConnectFail } from '@araminix/lib';
 *
 * const client = new Client({ host: 'https://app.example.com', wsHost: 'wss://app.example.com', connectTimeout: 10_000 });
 *
 * // The body is whatever your server's auth provider expects.
 * const login = await client.auth({ username: 'ada', password: 'correct horse battery staple' });
 * if (login.isErr()) throw new Error('login failed');
 *
 * const connected = await client.connect('platform');
 * if (connected.isErr()) {
 *     console.error(connected.e === ConnectFail.Unauthorized ? 'not logged in' : `cannot connect: ${connected.e}`);
 * } else {
 *     const socket = connected.unwrap();
 *     // open or accept streams on `socket`
 * }
 * ```
 */
export class Client {
    /** Normalised mount prefix: `''` or `'/segment[/segment...]'`. */
    private readonly mount: string;
    /** Token sent as `x-token`, if configured. */
    private readonly token?: string;
    /** Base URL for HTTP calls. */
    private readonly host: string;
    /** Base URL for WebSocket connections. */
    private readonly wsHost: string;
    /** Connection hp passed to every new connection (`undefined` means the default 300). */
    private readonly hp?: number;
    /** Connect timeout in milliseconds, if configured. */
    private readonly connectTimeout?: number;
    /** Reconnect limit, if configured. */
    private readonly maxReconnectAttempts?: number;

    /**
     * Creates a client. Nothing is requested or opened until you call a method.
     *
     * @param options - See {@link ClientOptions}.
     *
     * @example
     * ```ts
     * // Browser: cookie authentication, no token.
     * const browserClient = new Client({ host: 'https://app.example.com', wsHost: 'wss://app.example.com', mount: '/api' });
     *
     * // Node.js agent: token header.
     * const agentClient = new Client({ host: 'https://app.example.com', wsHost: 'wss://app.example.com', token: process.env.AGENT_TOKEN });
     * ```
     */
    constructor({ host, wsHost, mount, token, hp, connectTimeout, maxReconnectAttempts }: ClientOptions) {
        const trimmedMount = mount ? trimSlash(mount) : '';
        this.mount = trimmedMount ? `/${trimmedMount}` : '';
        this.token = token;
        this.host = host;
        this.wsHost = wsHost;
        this.hp = hp;
        this.connectTimeout = connectTimeout;
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    /** Headers of every HTTP call: `X-Requested-With`, plus `x-token` when a token is configured. */
    private requestHeaders(): Record<string, string> {
        // The server rejects cookie-authenticated mutations without JSON or this header (CSRF guard).
        const headers: Record<string, string> = { 'X-Requested-With': 'XMLHttpRequest' };
        if (this.token) {
            headers['x-token'] = this.token;
        }
        return headers;
    }

    /** Builds a full HTTP URL under `<host><mount>`. */
    private httpUrl(path: string): string {
        return `${this.host}${this.mount}${path}`;
    }

    /**
     * Logs in: `POST <host><mount>/auth` with `data` as the JSON body.
     *
     * On success a platform server either sets the httpOnly session cookie (used automatically by later calls and WebSocket
     * upgrades in browsers) or answers with a body, for example when a second factor is required.
     *
     * @typeParam TAuthData - Type of the request body, defined by the server's auth provider.
     * @typeParam TAuthResponse - Type of the response body.
     * @typeParam TErr - Type of the error; in practice what axios throws (an `AxiosError` for HTTP error statuses).
     * @param data - The credentials.
     * @returns `Ok(response)` with the full axios response, or `Err(error)` with what axios threw (including non-2xx responses).
     * Never rejects.
     *
     * @example
     * ```ts
     * const login = await client.auth({ username: 'ada', password: 'correct horse battery staple' });
     * if (login.isOk()) {
     *     console.log('logged in, status', login.unwrap().status);
     * }
     * ```
     */
    async auth<TAuthData = unknown, TAuthResponse = unknown, TErr = unknown>(data: TAuthData): Promise<Result<AxiosResponse<TAuthResponse>, TErr>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<TAuthResponse>, TErr>(axios.post(this.httpUrl('/auth'), data, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Logs in with Google: `POST <host><mount>/gauth` with `{ access_token }`.
     *
     * @typeParam TResponse - Type of the response body.
     * @typeParam TErr - Type of the error (what axios throws).
     * @param access_token - The Google OAuth access token obtained by your front end.
     * @returns `Ok(response)`; `Err(error)` with what axios threw; or `Err(undefined)` without any request when `access_token` is
     * empty. Never rejects.
     *
     * @example
     * ```ts
     * const login = await client.gauth(googleAccessToken);
     * if (login.isErr()) console.error('Google sign-in failed');
     * ```
     */
    async gauth<TResponse = unknown, TErr = unknown>(access_token: string): Promise<Result<AxiosResponse<TResponse>, TErr | void>> {
        if (!access_token) return new Err<void, AxiosResponse<TResponse>>(undefined);
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<TResponse>, TErr>(axios.post(this.httpUrl('/gauth'), { access_token }, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Logs out: `DELETE <host><mount>/auth`. The server ends the session.
     *
     * @returns `ok()` on success, `err()` on any failure (the error details are discarded). Never rejects.
     *
     * @example
     * ```ts
     * await client.deauth();
     * socket.destroy();
     * ```
     */
    async deauth(): Promise<Ok<void> | Err<void, void>> {
        const headers = this.requestHeaders();
        const res = await wrap(axios.delete(this.httpUrl('/auth'), { headers, withCredentials: true }));
        if (res.isOk()) return ok();
        return err();
    }

    /**
     * Starts two-factor enrolment for the logged-in user: `POST <host><mount>/2fa/request` without a body.
     *
     * @typeParam T - Type of the response body (a platform server answers `{ url }`, the provisioning URL to show the user).
     * @typeParam E - Type of the error (what axios throws).
     * @returns `Ok(response)` or `Err(error)`. Never rejects.
     *
     * @example
     * ```ts
     * const request = await client.request2FA<{ url: string }>();
     * if (request.isOk()) console.log('scan in an authenticator app:', request.unwrap().data.url);
     * ```
     */
    async request2FA<T = unknown, E = unknown>(): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.post(this.httpUrl('/2fa/request'), undefined, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Completes two-factor enrolment: `POST <host><mount>/2fa/enable` with `{ secret, code }`.
     *
     * @typeParam T - Type of the response body.
     * @typeParam E - Type of the error (what axios throws).
     * @param secret - The secret issued by {@link Client.request2FA}.
     * @param code - A current one-time code generated from that secret.
     * @returns `Ok(response)` or `Err(error)`. Never rejects.
     *
     * @example
     * ```ts
     * const enabled = await client.enable2FA(secret, '123456');
     * ```
     */
    async enable2FA<T = unknown, E = unknown>(secret: string, code: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.post(this.httpUrl('/2fa/enable'), { secret, code }, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Verifies a two-factor code during login: `POST <host><mount>/2fa/verify` with `{ accountName, code, uuid }`. On success a
     * platform server sets the session cookie.
     *
     * @typeParam T - Type of the response body.
     * @typeParam E - Type of the error (what axios throws).
     * @param accountName - The account that is logging in.
     * @param code - The one-time code entered by the user.
     * @param uuid - The identifier the server's auth provider expects for this verification.
     * @returns `Ok(response)` or `Err(error)`. Never rejects.
     *
     * @example
     * ```ts
     * const verified = await client.verify2FA('ada@example.com', '123456', challengeId);
     * ```
     */
    async verify2FA<T = unknown, E = unknown>(accountName: string, code: string, uuid: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.post(this.httpUrl('/2fa/verify'), { accountName, code, uuid }, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Turns two-factor authentication off: `DELETE <host><mount>/2fa/disable`, with `{ code }` as the body when `code` is given
     * and no body otherwise.
     *
     * @typeParam T - Type of the response body.
     * @typeParam E - Type of the error (what axios throws).
     * @param code - Optional current one-time code, for servers that require confirmation.
     * @returns `Ok(response)` or `Err(error)`. Never rejects.
     *
     * @example
     * ```ts
     * const disabled = await client.disable2FA('123456');
     * ```
     */
    async disable2FA<T = unknown, E = unknown>(code?: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(
            axios.delete(this.httpUrl('/2fa/disable'), {
                headers,
                withCredentials: true,
                data: code !== undefined ? { code } : undefined,
            }),
        );
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Uploads a file: `POST <host><mount>/file` as `multipart/form-data` with a single field named `file`.
     *
     * @param file - The file content. Pass a `Blob` (or `File`) for a real file upload. A string is appended as a plain text form
     * field, which servers expecting a file part (such as `@araminix/platform`) reject.
     * @returns `Ok(info)` with the server's {@link FileInfo}, or `Err(undefined)` on any failure (details are discarded). Never
     * rejects.
     *
     * @example
     * ```ts
     * const input = document.querySelector<HTMLInputElement>('input[type=file]')!;
     * const uploaded = await client.upload(input.files![0]);
     * if (uploaded.isOk()) console.log('stored as', uploaded.unwrap().id);
     * ```
     */
    async upload(file: string | Blob): Promise<Result<FileInfo, void>> {
        const form = new FormData();
        form.append('file', file);
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<FileInfo>>(axios.post(this.httpUrl('/file'), form, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v.data);
        return new Err<void, FileInfo>(undefined);
    }

    /**
     * Builds the download URL of an uploaded file. No request is made.
     *
     * @param file - A file id, giving `<host><mount>/file/<id>` (inserted as-is, not encoded), or a {@link FileInfo}, giving
     * `<host><mount>/file/<id>/<encoded name>`. The `FileInfo` should have an `id`.
     * @returns The absolute URL.
     *
     * @example
     * ```ts
     * client.getFileLink('f-1'); // 'https://app.example.com/api/file/f-1'
     * client.getFileLink({ id: 'f-1', name: 'my report.pdf', size: 1, mimetype: 'application/pdf', userId: 'u1' });
     * // 'https://app.example.com/api/file/f-1/my%20report.pdf'
     * ```
     */
    getFileLink(file: string | FileInfo): string {
        if (typeof file === 'string') {
            return this.httpUrl(`/file/${file}`);
        }
        return this.httpUrl(`/file/${file.id}/${encodeURIComponent(file.name)}`);
    }

    /**
     * Asks for a presigned URL to upload an attachment directly to object storage: `POST <host><mount>/attachments/upload` with
     * `{ mimetype, fileName }`.
     *
     * @typeParam T - Type of the response body (server-defined; typically the upload URL and the attachment key).
     * @typeParam E - Type of the error (what axios throws).
     * @param mimetype - MIME type of the file to upload.
     * @param fileName - Name of the file to upload.
     * @returns `Ok(response)` or `Err(error)`. Never rejects.
     *
     * @example
     * ```ts
     * const presigned = await client.getPresignedUploadUrl<{ url: string; key: string }>('application/pdf', 'invoice.pdf');
     * if (presigned.isOk()) {
     *     const { url, key } = presigned.unwrap().data;
     *     await fetch(url, { method: 'PUT', body: pdfBlob });
     *     console.log('stored as attachment', key);
     * }
     * ```
     */
    async getPresignedUploadUrl<T = unknown, E = unknown>(mimetype?: string, fileName?: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.post(this.httpUrl('/attachments/upload'), { mimetype, fileName }, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Asks for a presigned URL to download an attachment: `GET <host><mount>/attachments/<fileId>`, with `fileId` URL-encoded
     * (so keys containing `/` work).
     *
     * @typeParam T - Type of the response body (server-defined; typically the download URL).
     * @typeParam E - Type of the error (what axios throws).
     * @param fileId - The attachment key or id.
     * @returns `Ok(response)` or `Err(error)`. Never rejects.
     *
     * @example
     * ```ts
     * const presigned = await client.getPresignedDownloadUrl('u1/abc.pdf'); // GET .../attachments/u1%2Fabc.pdf
     * ```
     */
    async getPresignedDownloadUrl<T = unknown, E = unknown>(fileId: string): Promise<Result<AxiosResponse<T>, E>> {
        const headers = this.requestHeaders();
        const res = await wrap<AxiosResponse<T>, E>(axios.get(this.httpUrl(`/attachments/${encodeURIComponent(fileId)}`), { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Connects to the `agent` endpoint and wraps the socket in an {@link AgentClient}. Same as `connect('agent')` otherwise.
     *
     * @returns `Ok(agentClient)`, or `Err` with a {@link ConnectFail} reason (see {@link Client.connect}). Never rejects.
     *
     * @example
     * ```ts
     * const agentClient = new Client({ host: 'https://app.example.com', wsHost: 'wss://app.example.com', token: process.env.AGENT_TOKEN });
     * const connected = await agentClient.connectAgent();
     * if (connected.isOk()) {
     *     const agent = connected.unwrap();
     * }
     * ```
     */
    async connectAgent(): Promise<Result<AgentClient, ConnectFail>> {
        const socket = await this.connect('agent');
        if (socket.isErr()) {
            return new Err(socket.e);
        }
        return new Ok(new AgentClient(socket.unwrap()));
    }

    /**
     * Connects to the WebSocket endpoint `name` (optionally with `arg`) and resolves once the server has accepted it.
     *
     * The URL is `${wsHost}${mount}/ws/<name>` or `${wsHost}${mount}/ws/<name>/<arg>`; `name` and `arg` are URL-encoded, so pass
     * them unencoded. The returned {@link Socket} stays usable across network drops: the client reconnects in the background and
     * the connection resumes on the new WebSocket.
     *
     * Close codes are handled as follows (before and after acceptance):
     *
     * | Close code | `connect()` result | Connection closed with |
     * | --- | --- | --- |
     * | 4401 | `ConnectFail.Unauthorized` | `CloseCodes.Unauthorized` |
     * | 4403 | `ConnectFail.Denied` | `CloseCodes.Unauthorized` |
     * | 4404 | `ConnectFail.NotFound` | `CloseCodes.Unexpected` |
     * | 4409 | `ConnectFail.Concurrent` | `CloseCodes.Concurrent` |
     * | 4000 | `ConnectFail.Unexpected` | `CloseCodes.Intentional` |
     * | anything else | reconnect with backoff | (still open) |
     *
     * Reconnect delay: `min(30 s, 500 ms * 2^n) * random(0.5..1.5)`, where `n` counts consecutive reconnects. `n` goes back to 0
     * once a socket has stayed open for 5 seconds, and when the endpoint is first accepted. {@link ClientOptions.maxReconnectAttempts}
     * caps `n`, {@link ClientOptions.connectTimeout} caps the time until acceptance, and {@link ClientOptions.hp} caps the total
     * time spent without a socket. When any of these limits ends the attempt, `connect()` resolves with `ConnectFail.Unexpected`
     * (once accepted, the socket is destroyed instead).
     *
     * @param name - Endpoint name, e.g. `'platform'` or `'agent'`.
     * @param arg - Optional endpoint argument (for example a session or document id), available as {@link Socket.arg}.
     * @returns `Ok(socket)` once the server sends `accept_endpoint`, or `Err` with a {@link ConnectFail} reason. Never rejects.
     *
     * @example
     * ```ts
     * const connected = await client.connect('documents', 'doc-42');
     * if (connected.isErr()) {
     *     console.error('cannot open the document:', connected.e);
     * } else {
     *     const socket = connected.unwrap();
     *     const edits = await socket.outgoingStream<void, string, string>('edits');
     * }
     * ```
     */
    connect(name: string, arg?: string): Promise<Result<Socket, ConnectFail>> {
        const pathArg = arg ? `/${encodeURIComponent(arg)}` : '';
        const url = `${this.wsHost}${this.mount}/ws/${encodeURIComponent(name)}${pathArg}`;
        return connectEndpoint({
            url,
            arg: arg ?? '',
            token: this.token,
            hp: this.hp,
            connectTimeout: this.connectTimeout,
            maxReconnectAttempts: this.maxReconnectAttempts,
        });
    }
}
