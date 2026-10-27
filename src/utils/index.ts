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
    return str.replace(/^\/+|\/+$/g, '');
}
