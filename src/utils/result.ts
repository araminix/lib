export type Result<T, E> = Ok<T> | Err<E, T>;
export type R = Result<void, void>;

export function ok(): Ok<void> {
    return new Ok(undefined);
}

export function err(): Err<void, void> {
    return new Err(undefined);
}

export class Ok<T> {
    readonly v: T;

    constructor(v: T) {
        this.v = v;
    }

    isOk(): this is Ok<T> {
        return true;
    }

    isErr(): this is Err<never, T> {
        return false;
    }

    unwrap(): T {
        return this.v;
    }

    serialize(): { ok: true; v: T } {
        return {
            ok: true,
            v: this.v,
        };
    }
}

export class Err<E = void, T = void> {
    readonly e: E;

    constructor(e: E) {
        this.e = e;
    }

    isOk(): this is Ok<never> {
        return false;
    }

    isErr(): this is Err<E, never> {
        return true;
    }

    unwrap(): T {
        throw this.e;
    }

    serialize(): { ok: false; e: E } {
        return {
            ok: false,
            e: this.e,
        };
    }
}

export type Deal<T> = [promise: Promise<T>, resolve: (value: T) => R];

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
