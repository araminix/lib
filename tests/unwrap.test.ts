import assert from 'node:assert';
import { describe, it } from 'node:test';

import { Err, UnwrapError } from '../src/utils/result.js';

describe('Err.unwrap()', () => {
    it('rethrows Error values unchanged', () => {
        const error = new Error('boom');
        assert.throws(
            () => new Err(error).unwrap(),
            (thrown: unknown) => thrown === error,
        );
    });

    it('wraps non-Error values in an UnwrapError that keeps the original value', () => {
        assert.throws(
            () => new Err('NotFound').unwrap(),
            (thrown: unknown) => {
                assert.ok(thrown instanceof UnwrapError);
                assert.ok(thrown instanceof Error);
                assert.strictEqual(thrown.name, 'UnwrapError');
                assert.strictEqual(thrown.e, 'NotFound');
                assert.strictEqual(thrown.cause, 'NotFound');
                assert.match(thrown.message, /NotFound/);
                assert.ok(thrown.stack);
                return true;
            },
        );
    });

    it('describes objects and undefined in the message', () => {
        assert.throws(() => new Err({ code: 404 }).unwrap(), /\{"code":404\}/);
        assert.throws(() => new Err(undefined).unwrap(), /undefined/);
    });
});
