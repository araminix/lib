import assert from 'node:assert';
import { describe, it } from 'node:test';

import { AcceptEndpoint, AcceptStream, CloseStream, DataStream, NewStream, PingStream, PongStream } from '../src/messages.js';

describe('Messages Protocol Validation', () => {
    it('validates NewStream message', () => {
        const valid = { type: 'new_stream', id: 's-123', name: 'chat', args: { room: 1 } };
        assert.strictEqual(NewStream.is(valid), true);

        const invalid = { type: 'new_stream', id: 123 };
        assert.strictEqual(NewStream.is(invalid), false);
    });

    it('validates AcceptEndpoint message', () => {
        assert.strictEqual(AcceptEndpoint.is({ type: 'accept_endpoint' }), true);
        assert.strictEqual(AcceptEndpoint.is({ type: 'other' }), false);
    });

    it('validates AcceptStream message', () => {
        assert.strictEqual(AcceptStream.is({ type: 'accept_stream', id: 'abc' }), true);
        assert.strictEqual(AcceptStream.is({ type: 'accept_stream' }), false);
    });

    it('validates CloseStream message', () => {
        assert.strictEqual(CloseStream.is({ type: 'close_stream', id: 'abc', code: 'Normal' }), true);
        assert.strictEqual(CloseStream.is({ type: 'close_stream', id: 'abc' }), false);
    });

    it('validates DataStream message', () => {
        assert.strictEqual(DataStream.is({ type: 'data_stream', id: 'abc', data: [1, 2, 3] }), true);
        assert.strictEqual(DataStream.is({ type: 'data_stream', id: 'abc' }), false);
    });

    it('validates PingStream & PongStream messages', () => {
        assert.strictEqual(PingStream.is({ type: 'ping_stream', id: 'abc' }), true);
        assert.strictEqual(PongStream.is({ type: 'pong_stream', id: 'abc' }), true);
    });
});
