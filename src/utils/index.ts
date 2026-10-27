import * as t from 'io-ts';

import { logger } from './logger.js';
import { deal, Err, err, Ok, ok, R, Result } from './result.js';

export * from './logger.js';
export * from './result.js';

// Explicit re-exports: `t` is the io-ts namespace (the runtime type codecs used by the protocol messages), so applications can
// validate their own stream payloads with the same io-ts version; the Result helpers are repeated here for convenience.
export { t, Result, Ok, Err, ok, err, R, deal };

/**
 * What a {@link loop} body returns to decide whether the loop goes on.
 *
 * @example
 * ```ts
 * loop(async () => {
 *     const message = await stream.read();
 *     if (message.isErr()) return LoopResult.Stop;
 *     handle(message.unwrap());
 *     return LoopResult.Cont;
 * });
 * ```
 */
export enum LoopResult {
    /** Run the body again. */
    Cont = 0,
    /** Stop the loop. (Any value other than {@link LoopResult.Cont} also stops it.) */
    Stop = 1,
}

/**
 * Removes `undefined` from a type (distributes over unions), like `Exclude<T, undefined>`.
 *
 * @typeParam T - The type to filter.
 *
 * @example
 * ```ts
 * type Port = Defined<number | undefined>; // number
 * ```
 */
export type Defined<T> = T extends undefined ? never : T;

/**
 * Generates a random RFC 4122 version 4 UUID, e.g. `'3b241101-e2bb-4255-8caf-4136c566a962'`.
 *
 * Uses `crypto.randomUUID()` when available. That function only exists in secure contexts (HTTPS or localhost) in browsers, so
 * otherwise the UUID is built from `crypto.getRandomValues()`, which works everywhere. Stream ids are generated with this
 * function.
 *
 * @returns A lowercase UUID v4 string.
 *
 * @example
 * ```ts
 * import { uuid } from '@araminix/lib';
 *
 * const requestId = uuid();
 * ```
 */
export function uuid(): string {
    if (typeof crypto.randomUUID === 'function') {
        return crypto.randomUUID();
    }
    // randomUUID only exists in secure contexts (HTTPS/localhost); getRandomValues works everywhere.
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Resolves after `ms` milliseconds (a promisified `setTimeout`).
 *
 * @param ms - Delay in milliseconds.
 * @returns A promise that resolves with `undefined` after the delay. Never rejects.
 *
 * @example
 * ```ts
 * await sleep(250);
 * ```
 */
export function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Runs an async function over and over, one iteration at a time, until it returns {@link LoopResult.Stop}.
 *
 * Each iteration starts after the previous one has settled, so iterations never overlap. `loop()` returns immediately; the
 * iterations run in the background. Typical use is serving incoming streams or tasks:
 * `loop(async () => { const s = await socket.incomingStream('x'); ... })`.
 *
 * Errors never escape: if `fun` throws or rejects, the error is logged with {@link logger} (`'Unhandled loop error:'`), then
 * `onError` (if given) decides what happens next:
 * - no `onError`, or `onError` returns {@link LoopResult.Cont}: the loop restarts on the next macrotask (`setTimeout(..., 0)`);
 * - `onError` returns anything else (e.g. {@link LoopResult.Stop}): the loop ends;
 * - `onError` itself throws: that error is logged too and the loop ends.
 *
 * @param fun - The loop body. Return {@link LoopResult.Cont} to run again; any other value ends the loop.
 * @param onError - Optional error handler; may be async. Receives whatever `fun` threw or rejected with.
 *
 * @remarks
 * Successful iterations are chained as promise callbacks (microtasks), not timers. A body that keeps returning `Cont` without
 * awaiting real I/O (a read, a timer, a network call) never yields to the event loop and freezes the process; make sure each
 * iteration waits for something.
 *
 * @example
 * ```ts
 * import { LoopResult, loop, sleep } from '@araminix/lib';
 *
 * // `socket` is a connected Socket; processJob() is your own async handler.
 * loop(
 *     async () => {
 *         const incoming = await socket.incomingStream<{ id: string }>('jobs');
 *         if (incoming.isErr()) return LoopResult.Stop; // socket closed (or a newer acquirer took over)
 *         await processJob(incoming.unwrap());
 *         return LoopResult.Cont;
 *     },
 *     async () => {
 *         await sleep(1000); // back off after an unexpected exception (already logged), then keep serving
 *         return LoopResult.Cont;
 *     },
 * );
 * ```
 */
export function loop(fun: () => Promise<LoopResult>, onError?: (error: unknown) => LoopResult | Promise<LoopResult>): void {
    Promise.resolve()
        .then(fun)
        .then((result) => {
            if (result === LoopResult.Cont) {
                loop(fun, onError);
            }
        })
        .catch(async (error: unknown) => {
            logger.error('Unhandled loop error:', error);
            try {
                const nextAction = onError ? await onError(error) : LoopResult.Cont;
                if (nextAction === LoopResult.Cont) {
                    setTimeout(() => {
                        loop(fun, onError);
                    }, 0);
                }
            } catch (handlerError) {
                logger.error('Error inside loop onError handler:', handlerError);
            }
        });
}

/**
 * Runs a function once, fire-and-forget, without ever producing an uncaught exception or unhandled rejection.
 *
 * `fun` is called synchronously. A synchronous throw is caught, and if `fun` returns a promise (or any thenable with a `catch`
 * method) its rejection is caught as well; both are logged with {@link logger} as `'Unhandled once error:'`. Use it to start
 * background work from synchronous code.
 *
 * @param fun - The function to run. Its return value is ignored.
 *
 * @example
 * ```ts
 * import { once } from '@araminix/lib';
 *
 * once(async () => {
 *     const code = await socket.waitClose();
 *     console.log('socket closed:', code);
 * });
 * ```
 */
export function once(fun: () => Promise<void> | void): void {
    try {
        const res = fun();
        if (res && typeof (res as any).catch === 'function') {
            (res as Promise<void>).catch((error: unknown) => {
                logger.error('Unhandled once error:', error);
            });
        }
    } catch (error) {
        logger.error('Unhandled once error:', error);
    }
}

/**
 * Awaits a promise (or calls an async function) and turns its outcome into a {@link Result}.
 *
 * A fulfilled value becomes `Ok(value)`; a rejection, or a synchronous throw from `exe`, becomes `Err(error)`.
 *
 * @typeParam OkType - Type of the fulfilled value.
 * @typeParam ErrType - Type the caught error is cast to (not checked at runtime). Defaults to `Error`.
 * @param exe - A `Promise`, or a function returning one. A function is called inside the `try`, so synchronous throws are caught too.
 * @returns A promise of `Ok(value)` or `Err(error)`. Never rejects.
 *
 * @remarks
 * Only real `Promise` instances are awaited directly; any other value is called as a function, so pass foreign thenables as
 * `() => thenable`.
 *
 * @example
 * ```ts
 * import { wrap } from '@araminix/lib';
 *
 * const response = await wrap(fetch('https://api.example.com/health'));
 * if (response.isOk()) {
 *     console.log('status', response.v.status);
 * } else {
 *     console.error('network failure', response.e.message);
 * }
 * ```
 */
export async function wrap<OkType, ErrType = Error>(exe: Promise<OkType> | (() => Promise<OkType>)): Promise<Result<OkType, ErrType>> {
    try {
        if (exe instanceof Promise) {
            return new Ok(await exe);
        }
        return new Ok(await exe());
    } catch (e) {
        return new Err(e as ErrType);
    }
}

/**
 * Calls a synchronous function and turns its outcome into a {@link Result}: the return value becomes `Ok`, a throw becomes `Err`.
 *
 * @typeParam OkType - Type of the return value.
 * @typeParam ErrType - Type the caught error is cast to (not checked at runtime). Defaults to `Error`.
 * @param exe - The function to call.
 * @returns `Ok(returnValue)` or `Err(thrownValue)`. Never throws.
 *
 * @example
 * ```ts
 * import { wrapSync } from '@araminix/lib';
 *
 * const parsed = wrapSync<{ name: string }, SyntaxError>(() => JSON.parse(text));
 * if (parsed.isErr()) console.error('bad JSON:', parsed.e.message);
 * ```
 */
export function wrapSync<OkType, ErrType = Error>(exe: () => OkType): Result<OkType, ErrType> {
    try {
        return new Ok(exe());
    } catch (e) {
        return new Err(e as ErrType);
    }
}

/**
 * Removes all leading and trailing `/` characters. Slashes inside the string are kept.
 *
 * @param str - The string to trim.
 * @returns The trimmed string (`''` if it consisted only of slashes).
 *
 * @example
 * ```ts
 * trimSlash('//api/v1//'); // 'api/v1'
 * trimSlash('///'); // ''
 * ```
 */
export function trimSlash(str: string): string {
    return str.replace(/^\/+|\/+$/g, '');
}
