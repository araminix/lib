import { CloseCodes } from '../connection/index.js';
import { Socket, SocketErr } from '../socket/index.js';
import { once } from '../utils/helpers.js';
import { deal, Err, Ok, R, Result } from '../utils/result.js';
import { AgentClientError, CommunicationClientAgentError, ExecutionClientAgentError } from './agentErrors.js';

/**
 * Request/response helper for the `agent` endpoint; get one from {@link Client.connectAgent}.
 *
 * It works in both directions:
 * - {@link AgentClient.post} sends a named request to the server and waits for its reply;
 * - {@link AgentClient.onTask} receives tasks the server sends to this agent and answers them with a {@link Result}.
 *
 * Replies travel as serialized results: `{ ok: true, v }` or `{ ok: false, e }` (see `Ok.serialize()` / `Err.serialize()`).
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
