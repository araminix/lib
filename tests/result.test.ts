import assert from 'node:assert';
import { describe, it } from 'node:test';

import { deal, err, Err, ok, Ok } from '../src/utils/result.js';

describe('Result Monad', () => {
    it('ok() creates an Ok<void> instance', () => {
        const r = ok();
        assert.strictEqual(r.isOk(), true);
        assert.strictEqual(r.isErr(), false);
        assert.strictEqual(r.unwrap(), undefined);
        assert.deepStrictEqual(r.serialize(), { ok: true, v: undefined });
    });

    it('err() creates an Err<void> instance', () => {
        const r = err();
        assert.strictEqual(r.isOk(), false);
        assert.strictEqual(r.isErr(), true);
        assert.throws(() => r.unwrap());
        assert.deepStrictEqual(r.serialize(), { ok: false, e: undefined });
    });

    it('Ok<T> properly handles typed values', () => {
        const r = new Ok({ name: 'folkrum', score: 100 });
        assert.strictEqual(r.isOk(), true);
        assert.deepStrictEqual(r.unwrap(), { name: 'folkrum', score: 100 });
    });

    it('Err<E> properly handles typed errors', () => {
        const errorObj = new Error('Custom Error');
        const r = new Err(errorObj);
        assert.strictEqual(r.isErr(), true);
        assert.strictEqual(r.e, errorObj);
    });

    it('deal<T>() resolves promise on call', async () => {
        const [promise, resolve] = deal<string>();
        const res = resolve('resolved value');
        assert.strictEqual(res.isOk(), true);
        const value = await promise;
        assert.strictEqual(value, 'resolved value');
    });

    it('deal<T>() only allows single resolution', async () => {
        const [promise, resolve] = deal<number>();
        const first = resolve(42);
        assert.strictEqual(first.isOk(), true);
        const second = resolve(99);
        assert.strictEqual(second.isErr(), true);
        const value = await promise;
        assert.strictEqual(value, 42);
    });
});
