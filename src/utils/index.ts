import * as t from 'io-ts';

import { logger } from './logger.js';
import { deal, Err, err, Ok, ok, R, Result } from './result.js';

export * from './logger.js';
export * from './result.js';

export { t, Result, Ok, Err, ok, err, R, deal };

export enum LoopResult {
    Cont = 0,
    Stop = 1,
}

export type Defined<T> = T extends undefined ? never : T;

export function uuid(): string {
    return crypto.randomUUID();
}

export function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

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

export function wrapSync<OkType, ErrType = Error>(exe: () => OkType): Result<OkType, ErrType> {
    try {
        return new Ok(exe());
    } catch (e) {
        return new Err(e as ErrType);
    }
}

export function trimSlash(str: string): string {
    let newStr = str;
    if (newStr.startsWith('/')) {
        newStr = newStr.slice(1);
    }
    if (newStr.endsWith('/')) {
        newStr = newStr.slice(0, -1);
    }
    return newStr;
}
