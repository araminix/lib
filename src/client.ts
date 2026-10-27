import axios, { AxiosResponse } from 'axios';

import { CloseCodes, Connection, MinimalWebSocket, WsCloseCodes } from './connection.js';
import { Socket, SocketErr } from './Socket.js';
import { err, logger, ok, once, R, trimSlash, wrap } from './utils/index.js';
import { deal, Err, Ok, Result } from './utils/result.js';

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

/** Milliseconds a reopened socket must stay open before the reconnect backoff resets. */
const RECONNECT_STABLE_MS = 5000; // a reopened socket must stay up this long before the backoff resets

/** Constructor signature of the global `WebSocket`; Node's built-in implementation also accepts `{ headers }`. */
type WebSocketConstructor = new (url: string, init?: { headers: Record<string, string> }) => MinimalWebSocket;

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

/**
 * Discriminator of the errors returned by {@link AgentClient.post}.
 *
 * @example
 * ```ts
 * if (reply.isErr() && reply.e.type === AgentClientErrorType.Execution) {
 *     console.error('the request reached the server but failed there');
 * }
 * ```
 */
export enum AgentClientErrorType {
    /** The exchange did not complete: the stream could not be opened, no valid reply arrived in time, or the stream closed. */
    Communication = 'Communication',
    /** The server handled the request and answered with an error (`{ ok: false, e }`). */
    Execution = 'Execution',
}

/**
 * Common type of the errors returned by {@link AgentClient.post}: {@link CommunicationClientAgentError} or
 * {@link ExecutionClientAgentError}.
 *
 * @remarks
 * The two concrete classes `implement` this abstract class instead of extending it, so `error instanceof AgentClientError` is
 * always `false`. Use {@link AgentClientError.type}, or `instanceof` with a concrete class.
 *
 * @example
 * ```ts
 * const reply = await agent.post('summarize', { text });
 * if (reply.isErr()) {
 *     const error = reply.e;
 *     if (error instanceof ExecutionClientAgentError) {
 *         console.error('server-side failure:', error.e);
 *     } else if (error instanceof CommunicationClientAgentError) {
 *         console.error('no valid reply:', error.e);
 *     }
 * }
 * ```
 */
export abstract class AgentClientError {
    /** Which kind of failure this is. */
    abstract type: AgentClientErrorType;
}

/**
 * {@link AgentClient.post} could not complete the exchange.
 *
 * {@link CommunicationClientAgentError.e} tells why: an `InitStreamErr` (the `post` stream could not be opened), a `StreamErr`
 * (`Timeout`: no reply within `hp` ticks; `Dead`: the stream closed without a reply), or the string `'Format'` (the reply was not
 * an object with an `ok` field).
 *
 * @typeParam E - Type of the cause.
 *
 * @example
 * ```ts
 * import { CommunicationClientAgentError, StreamErr } from '@araminix/lib';
 *
 * const reply = await agent.post('report', { month: '2026-10' }, 30);
 * if (reply.isErr() && reply.e instanceof CommunicationClientAgentError && reply.e.e === StreamErr.Timeout) {
 *     console.warn('no answer within 30 s');
 * }
 * ```
 */
export class CommunicationClientAgentError<E = unknown> implements AgentClientError {
    /** The cause: an `InitStreamErr`, a `StreamErr`, or `'Format'`. */
    readonly e: E;
    /** Always {@link AgentClientErrorType.Communication}. */
    readonly type = AgentClientErrorType.Communication;

    /**
     * Creates the error.
     *
     * @param e - The cause.
     */
    constructor(e: E) {
        this.e = e;
    }
}

/**
 * The server handled an {@link AgentClient.post} request and replied with an error.
 *
 * {@link ExecutionClientAgentError.e} is the `e` of the `{ ok: false, e }` reply, e.g. the value the handler passed to
 * `respond(new Err(...))` in {@link AgentClient.onTask}.
 *
 * @typeParam E - Type of the error value.
 *
 * @example
 * ```ts
 * const reply = await agent.post<{ id: number }, { name: string }, string>('lookup', { id: 7 });
 * if (reply.isErr() && reply.e instanceof ExecutionClientAgentError) {
 *     console.error('lookup failed:', reply.e.e);
 * }
 * ```
 */
export class ExecutionClientAgentError<E = unknown> implements AgentClientError {
    /** The error value reported by the server. */
    readonly e: E;
    /** Always {@link AgentClientErrorType.Execution}. */
    readonly type = AgentClientErrorType.Execution;

    /**
     * Creates the error.
     *
     * @param e - The error value from the reply.
     */
    constructor(e: E) {
        this.e = e;
    }
}

/**
 * Request/response helper for the `agent` endpoint; get one from {@link Client.connectAgent}.
 *
 * It works in both directions:
 * - {@link AgentClient.post} sends a named request to the server and waits for its reply;
 * - {@link AgentClient.onTask} receives tasks the server sends to this agent and answers them with a {@link Result}.
 *
 * Replies travel as serialised results: `{ ok: true, v }` or `{ ok: false, e }` (see `Ok.serialize()` / `Err.serialize()`).
 *
 * @example
 * ```ts
 * import { Client, Err, LoopResult, Ok, loop, once } from '@araminix/lib';
 *
 * declare function resize(url: string): Promise<number>; // your own work
 *
 * const client = new Client({ host: 'https://api.example.com', wsHost: 'wss://api.example.com', token: process.env.AGENT_TOKEN });
 * const connected = await client.connectAgent();
 * if (connected.isErr()) throw new Error(`agent connection failed: ${connected.e}`);
 * const agent = connected.unwrap();
 *
 * // Serve "resize" tasks: acquire the next task right away and handle each one in the background.
 * loop(async () => {
 *     const task = await agent.onTask<{ url: string }, { width: number }, string>('resize');
 *     if (task.isErr()) return LoopResult.Stop;
 *     const [input, respond] = task.unwrap();
 *     once(async () => {
 *         try {
 *             respond(new Ok({ width: await resize(input.url) }));
 *         } catch (error) {
 *             respond(new Err(String(error)));
 *         }
 *     });
 *     return LoopResult.Cont;
 * });
 * ```
 */
export class AgentClient {
    /** The socket of the `agent` endpoint. */
    private socket: Socket;

    /**
     * Wraps a socket connected to the `agent` endpoint. Usually created for you by {@link Client.connectAgent}.
     *
     * @param socket - A socket of the `agent` endpoint.
     *
     * @example
     * ```ts
     * const connected = await client.connect('agent');
     * if (connected.isOk()) {
     *     const agent = new AgentClient(connected.unwrap());
     * }
     * ```
     */
    constructor(socket: Socket) {
        this.socket = socket;
    }

    /**
     * Waits for the next task called `name` from the server and returns its input together with a one-shot `respond` function.
     *
     * Under the hood it accepts the next incoming stream called `name` (see {@link Socket.incomingStream}); the stream's `args`
     * are the task input. `respond(result)` writes `result.serialize()` to that stream; the requester reads it and closes the
     * stream. Only the first call counts: `respond` returns `ok()` the first time and `err()` afterwards.
     *
     * Tasks that arrive while no `onTask()` call is waiting are queued for `streamRequestHp` ticks (10 s) and then dropped, so ask
     * for the next task promptly (typically in a `loop()`) and do slow work in the background. Only one call per name can wait
     * at a time; a second one makes the first resolve with {@link SocketErr.Concurrent}.
     *
     * @typeParam TTask - Type of the task input. Not validated at runtime.
     * @typeParam TResult - Type of a successful answer.
     * @typeParam TError - Type of an error answer.
     * @param name - The task (stream) name.
     * @returns `Ok([input, respond])`; `Err(SocketErr.Closed)` if the connection is or becomes dead; `Err(SocketErr.Concurrent)` if
     * a newer call for the same name replaced this one. Never rejects.
     *
     * @remarks
     * The answer is written in the background: if the stream is already gone (the requester timed out) it is dropped silently. If
     * `respond` is never called, the requester's read times out.
     *
     * @example
     * ```ts
     * const task = await agent.onTask<{ a: number; b: number }, number, string>('add');
     * if (task.isOk()) {
     *     const [input, respond] = task.unwrap();
     *     const sum = input.a + input.b;
     *     respond(Number.isFinite(sum) ? new Ok(sum) : new Err('invalid input'));
     * }
     * ```
     */
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

    /**
     * Closes the agent connection for good ({@link Socket.destroy}): `CloseCodes.Intentional`, no more reconnects.
     *
     * @example
     * ```ts
     * process.once('SIGTERM', () => agent.destroy());
     * ```
     */
    destroy(): void {
        this.socket.destroy();
    }

    /**
     * Sends a request called `name` with `data` to the server and waits for the reply.
     *
     * Opens an outgoing stream named `post` whose arguments are `{ name, data }`, then reads one message with `read(hp)`. A
     * `{ ok: true, v }` reply becomes `Ok(v)` and `{ ok: false, e }` becomes `Err(ExecutionClientAgentError(e))`. Anything else,
     * and any failure to open the stream or read the reply, becomes `Err(CommunicationClientAgentError(cause))`.
     *
     * @typeParam TData - Type of the request payload.
     * @typeParam TResult - Type of a successful reply value.
     * @typeParam E - Type of an error reply value.
     * @param name - Request name, interpreted by the server's `post` handler.
     * @param data - JSON-serialisable payload.
     * @param hp - Reply timeout in ticks (seconds), counted after the stream was opened. Defaults to 10. Opening the stream has its
     * own limit of about 20 ticks.
     * @returns `Ok(value)` or `Err` with an {@link AgentClientError}. Never rejects.
     *
     * @remarks
     * After a valid reply the server closes the `post` stream. When the read times out or the reply is malformed, the client
     * closes the stream with `Error` itself, so nothing is left open.
     *
     * @example
     * ```ts
     * const reply = await agent.post<{ text: string }, { summary: string }, string>('summarize', { text: article }, 30);
     * if (reply.isOk()) {
     *     console.log(reply.unwrap().summary);
     * } else if (reply.e instanceof ExecutionClientAgentError) {
     *     console.error('the server refused:', reply.e.e);
     * } else {
     *     console.error('no valid reply:', (reply.e as CommunicationClientAgentError).e);
     * }
     * ```
     */
    async post<TData = unknown, TResult = unknown, E = unknown>(name: string, data: TData, hp: number = 10): Promise<Result<TResult, AgentClientError>> {
        const stream = await this.socket.outgoingStream<{ name: string; data: TData }, { ok: boolean; v: TResult; e: E }>('post', { name, data });
        if (stream.isErr()) {
            return new Err(new CommunicationClientAgentError(stream.e));
        }
        const readResult = await stream.unwrap().read<{ ok: boolean; v: TResult; e: E }>(hp);
        if (readResult.isErr()) {
            stream.unwrap().closeErr();
            return new Err(new CommunicationClientAgentError(readResult.e));
        }
        const read = readResult.unwrap();
        if (typeof read !== 'object' || read === null || !('ok' in read)) {
            stream.unwrap().closeErr();
            return new Err(new CommunicationClientAgentError('Format'));
        }
        if (read.ok === true) {
            return new Ok(read.v);
        }
        return new Err(new ExecutionClientAgentError(read.e));
    }

    /**
     * Resolves once the agent connection is destroyed; see {@link Socket.waitClose}.
     *
     * @returns A promise of the `CloseCodes` reason. Never rejects.
     *
     * @example
     * ```ts
     * const reason = await agent.waitClose();
     * console.log('agent disconnected for good:', reason);
     * ```
     */
    async waitClose(): Promise<CloseCodes> {
        return await this.socket.waitClose();
    }
}

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
        const res = await wrap<AxiosResponse<TAuthResponse>, TErr>(axios.post(`${this.host}${this.mount}/auth`, data, { headers, withCredentials: true }));
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
        const res = await wrap<AxiosResponse<TResponse>, TErr>(axios.post(`${this.host}${this.mount}/gauth`, { access_token }, { headers, withCredentials: true }));
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
        const res = await wrap(axios.delete(`${this.host}${this.mount}/auth`, { headers, withCredentials: true }));
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
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/request`, undefined, { headers, withCredentials: true }));
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
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/enable`, { secret, code }, { headers, withCredentials: true }));
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
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/2fa/verify`, { accountName, code, uuid }, { headers, withCredentials: true }));
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
            axios.delete(`${this.host}${this.mount}/2fa/disable`, {
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
        const res = await wrap<AxiosResponse<FileInfo>>(axios.post(`${this.host}${this.mount}/file`, form, { headers, withCredentials: true }));
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
            return `${this.host}${this.mount}/file/${file}`;
        }
        return `${this.host}${this.mount}/file/${file.id}/${encodeURIComponent(file.name)}`;
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
        const res = await wrap<AxiosResponse<T>, E>(axios.post(`${this.host}${this.mount}/attachments/upload`, { mimetype, fileName }, { headers, withCredentials: true }));
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
        const res = await wrap<AxiosResponse<T>, E>(axios.get(`${this.host}${this.mount}/attachments/${encodeURIComponent(fileId)}`, { headers, withCredentials: true }));
        if (res.isOk()) return new Ok(res.v);
        return new Err(res.e);
    }

    /**
     * Opens a new WebSocket for `url` and attaches it to `connection` without refilling hp (that happens when it opens). Does
     * nothing if the connection is dead; destroys it with `Unexpected` if no WebSocket can be created.
     */
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
                if (connection.dead) {
                    // A close that follows destroy() (e.g. after connectTimeout) must not schedule a reconnect.
                    return;
                }
                switch (code) {
                    case WsCloseCodes.ConcurrentConnection: {
                        clearTimers();
                        resolve(new Err(ConnectFail.Concurrent));
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
