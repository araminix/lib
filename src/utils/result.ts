/**
 * The outcome of an operation that can fail: either {@link Ok} with a value or {@link Err} with an error.
 *
 * Every fallible API in this package returns a `Result` (usually inside a `Promise`) instead of throwing, so failures are
 * part of the type and cannot be forgotten. Narrow with {@link Ok.isOk} / {@link Err.isErr}, then read `v` or `e`, or call
 * `unwrap()` when a failure should become an exception.
 *
 * @typeParam T - Type of the success value.
 * @typeParam E - Type of the error value.
 *
 * @example
 * ```ts
 * import { Err, Ok, Result } from '@araminix/lib';
 *
 * function parsePort(input: string): Result<number, string> {
 *     const port = Number(input);
 *     return Number.isInteger(port) && port > 0 && port < 65536 ? new Ok(port) : new Err(`invalid port: ${input}`);
 * }
 *
 * const port = parsePort('8080');
 * if (port.isOk()) {
 *     console.log(port.v + 1);
 * } else {
 *     console.error(port.e);
 * }
 * ```
 */
export type Result<T, E> = Ok<T> | Err<E, T>;
/**
 * Shorthand for `Result<void, void>`: success or failure without any payload, as returned by {@link ok} and {@link err}.
 *
 * Used throughout the library for "did it work?" answers, such as the low-level `Connection.write*` methods and the resolver
 * returned by {@link deal}.
 *
 * @example
 * ```ts
 * import { R, err, ok } from '@araminix/lib';
 *
 * function save(value: string): R {
 *     return value.length > 0 ? ok() : err();
 * }
 * ```
 */
export type R = Result<void, void>;

/**
 * Creates a payload-less success: `new Ok(undefined)`.
 *
 * @returns An `Ok<void>`.
 *
 * @example
 * ```ts
 * const result: R = ok();
 * result.isOk(); // true
 * ```
 */
export function ok(): Ok<void> {
    return new Ok(undefined);
}

/**
 * Creates a payload-less failure: `new Err(undefined)`.
 *
 * @returns An `Err<void, void>`. Calling `unwrap()` on it throws an {@link UnwrapError}.
 *
 * @example
 * ```ts
 * const result: R = err();
 * result.isErr(); // true
 * ```
 */
export function err(): Err<void, void> {
    return new Err(undefined);
}

/**
 * The success variant of {@link Result}, holding the value in {@link Ok.v}.
 *
 * @typeParam T - Type of the value.
 *
 * @example
 * ```ts
 * const result = new Ok({ id: 7 });
 * result.isOk(); // true
 * result.unwrap().id; // 7
 * result.serialize(); // { ok: true, v: { id: 7 } }
 * ```
 */
export class Ok<T> {
    /** The success value. */
    readonly v: T;

    /**
     * Wraps a success value.
     *
     * @param v - The value to wrap.
     */
    constructor(v: T) {
        this.v = v;
    }

    /**
     * Type guard that is always `true` for an `Ok`; narrows a {@link Result} to `Ok<T>`.
     *
     * @returns `true`.
     */
    isOk(): this is Ok<T> {
        return true;
    }

    /**
     * Type guard that is always `false` for an `Ok`. On a {@link Result}, the `false` branch is narrowed to `Ok<T>`.
     *
     * @returns `false`.
     */
    isErr(): this is Err<never, T> {
        return false;
    }

    /**
     * Returns the value. Never throws for an `Ok`.
     *
     * @returns {@link Ok.v}.
     *
     * @example
     * ```ts
     * new Ok(42).unwrap(); // 42
     * ```
     */
    unwrap(): T {
        return this.v;
    }

    /**
     * Converts the result to a plain, JSON-friendly object, e.g. to send it over a stream.
     *
     * This is the reply format that `AgentClient.onTask()` writes and `AgentClient.post()` reads.
     *
     * @returns `{ ok: true, v }`.
     *
     * @example
     * ```ts
     * JSON.stringify(new Ok(3).serialize()); // '{"ok":true,"v":3}'
     * ```
     */
    serialize(): { ok: true; v: T } {
        return {
            ok: true,
            v: this.v,
        };
    }
}

/**
 * The failure variant of {@link Result}, holding the error in {@link Err.e}.
 *
 * @typeParam E - Type of the error value. Defaults to `void`.
 * @typeParam T - Type of the success value of the {@link Result} this error stands in for; only used for typing `unwrap()`.
 *
 * @example
 * ```ts
 * const result: Result<number, string> = new Err('NotFound');
 * if (result.isErr()) {
 *     console.error(result.e); // 'NotFound'
 * }
 * ```
 */
export class Err<E = void, T = void> {
    /** The error value. Any type: an `Error`, an enum member such as `StreamErr.Timeout`, a string, `undefined`, ... */
    readonly e: E;

    /**
     * Wraps an error value.
     *
     * @param e - The error value to wrap.
     */
    constructor(e: E) {
        this.e = e;
    }

    /**
     * Type guard that is always `false` for an `Err`. On a {@link Result}, the `false` branch is narrowed to the `Err` variant.
     *
     * @returns `false`.
     */
    isOk(): this is Ok<never> {
        return false;
    }

    /**
     * Type guard that is always `true` for an `Err`; narrows a {@link Result} to `Err<E, never>`.
     *
     * @returns `true`.
     */
    isErr(): this is Err<E, never> {
        return true;
    }

    /**
     * Throws, because there is no value to return.
     *
     * If {@link Err.e} is an `Error` it is rethrown unchanged (same object, same stack). Any other value (a string code, an enum
     * member, `undefined`, a plain object, ...) is wrapped in an {@link UnwrapError} whose `e` and `cause` hold the original value.
     *
     * @returns Never returns.
     * @throws `Error` - {@link Err.e} itself when it is an `Error` instance.
     * @throws {@link UnwrapError} - When {@link Err.e} is not an `Error` instance.
     *
     * @remarks
     * Changed in 3.0.0: earlier versions threw the raw value. Code that catches `unwrap()` and compares the thrown value with an
     * error code must now read `error.e` (or `error.cause`) from the `UnwrapError`.
     *
     * @example
     * ```ts
     * try {
     *     new Err('NotFound').unwrap();
     * } catch (error) {
     *     if (error instanceof UnwrapError && error.e === 'NotFound') {
     *         // handle the missing item
     *     }
     * }
     * ```
     */
    unwrap(): T {
        throw this.e instanceof Error ? this.e : new UnwrapError(this.e);
    }

    /**
     * Converts the result to a plain, JSON-friendly object, e.g. to send it over a stream.
     *
     * @returns `{ ok: false, e }`. Note that an `Error` value serialises to `{}` with `JSON.stringify`; send a string or plain
     * object if the peer needs the details.
     *
     * @example
     * ```ts
     * JSON.stringify(new Err('busy').serialize()); // '{"ok":false,"e":"busy"}'
     * ```
     */
    serialize(): { ok: false; e: E } {
        return {
            ok: false,
            e: this.e,
        };
    }
}

/** Renders an arbitrary error value for the {@link UnwrapError} message: strings as-is, everything else as JSON or `String()`. */
function describeValue(value: unknown): string {
    if (typeof value === 'string') {
        return value;
    }
    try {
        return JSON.stringify(value) ?? String(value);
    } catch {
        return String(value);
    }
}

/**
 * The exception {@link Err.unwrap} throws when the error value is not an `Error`.
 *
 * It gives non-`Error` failures (string codes, enum members, `undefined`, plain objects) a stack trace and a readable message,
 * while keeping the original value in {@link UnwrapError.e} and in the standard `cause` property.
 *
 * - `name` is `'UnwrapError'`.
 * - `message` is `unwrap() called on an Err: <value>`, where strings appear as-is and other values as JSON (or `String(value)`).
 *
 * @typeParam E - Type of the original error value.
 *
 * @example
 * ```ts
 * import { ConnectFail, UnwrapError } from '@araminix/lib';
 *
 * // `client` is a Client instance
 * try {
 *     const socket = (await client.connect('chat')).unwrap();
 *     console.log('connected', socket.arg);
 * } catch (error) {
 *     if (error instanceof UnwrapError && error.e === ConnectFail.Unauthorized) {
 *         console.warn('session expired, please log in again');
 *     } else {
 *         throw error;
 *     }
 * }
 * ```
 */
export class UnwrapError<E = unknown> extends Error {
    /** The original error value of the {@link Err} (also available as `cause`). */
    readonly e: E;

    /**
     * Creates the error for a non-`Error` value.
     *
     * @param e - The original error value; stored in {@link UnwrapError.e} and `cause`.
     */
    constructor(e: E) {
        super(`unwrap() called on an Err: ${describeValue(e)}`, { cause: e });
        this.name = 'UnwrapError';
        this.e = e;
    }
}

/**
 * The tuple returned by {@link deal}: a promise and the function that settles it.
 *
 * `resolve(value)` returns `ok()` the first time and `err()` on every later call (the promise keeps its first value).
 *
 * @typeParam T - Type of the value the promise resolves with.
 */
export type Deal<T> = [promise: Promise<T>, resolve: (value: T) => R];

/**
 * Creates a promise together with a resolver that can be called from anywhere (a "deferred").
 *
 * The resolver settles the promise only once: the first call returns `ok()`, later calls do nothing and return `err()`, which
 * makes it safe to race several completion paths (timeout, success, shutdown) against each other. The promise never rejects;
 * model failures with a {@link Result} as `T`.
 *
 * @typeParam T - Type of the value the promise resolves with.
 * @returns `[promise, resolve]`, see {@link Deal}.
 *
 * @example
 * ```ts
 * import { EventEmitter } from 'node:events';
 * import { Err, Ok, Result, deal } from '@araminix/lib';
 *
 * const events = new EventEmitter();
 * const [answer, resolve] = deal<Result<string, 'Timeout'>>();
 * const timer = setTimeout(() => resolve(new Err('Timeout')), 5000);
 * events.once('message', (text: string) => resolve(new Ok(text)));
 *
 * const result = await answer; // whichever happened first wins; the other resolve() call returns err()
 * clearTimeout(timer);
 * ```
 */
export function deal<T>(): Deal<T> {
    let resolve!: (result: T) => R;
    const promise = new Promise<T>((res) => {
        let pending = true;
        resolve = (result: T): R => {
            if (!pending) {
                return err();
            }
            pending = false;
            res(result);
            return ok();
        };
    });
    return [promise, resolve];
}
