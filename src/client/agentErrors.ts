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
