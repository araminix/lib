import assert from 'node:assert';
import { describe, it } from 'node:test';

import { once, sleep, trimSlash, uuid, wrap, wrapSync } from '../src/utils/index.js';

describe('Utils', () => {
    it('uuid() returns valid UUID v4 format', () => {
        const id = uuid();
        assert.strictEqual(typeof id, 'string');
        assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    });

    it('trimSlash() trims leading and trailing slashes correctly', () => {
        assert.strictEqual(trimSlash('/api/v1/'), 'api/v1');
        assert.strictEqual(trimSlash('/path'), 'path');
        assert.strictEqual(trimSlash('path/'), 'path');
        assert.strictEqual(trimSlash('clean'), 'clean');
    });

    it('wrapSync() wraps successful execution in Ok', () => {
        const result = wrapSync(() => 10 + 5);
        assert.strictEqual(result.isOk(), true);
        assert.strictEqual(result.unwrap(), 15);
    });

    it('wrapSync() catches thrown exceptions in Err', () => {
        const result = wrapSync(() => {
            throw new Error('boom');
        });
        assert.strictEqual(result.isErr(), true);
        if (result.isErr()) {
            assert.strictEqual((result.e as Error).message, 'boom');
        }
    });

    it('wrap() wraps successful Promise in Ok', async () => {
        const result = await wrap(Promise.resolve('async ok'));
        assert.strictEqual(result.isOk(), true);
        assert.strictEqual(result.unwrap(), 'async ok');
    });

    it('wrap() catches rejected Promise in Err', async () => {
        const result = await wrap(Promise.reject(new Error('async fail')));
        assert.strictEqual(result.isErr(), true);
    });

    it('sleep() delays execution', async () => {
        const start = Date.now();
        await sleep(50);
        const elapsed = Date.now() - start;
        assert.ok(elapsed >= 40);
    });

    it('once() executes synchronous function', () => {
        let count = 0;
        once(() => {
            count++;
        });
        assert.strictEqual(count, 1);
    });
});
