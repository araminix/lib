import assert from 'node:assert';
import { describe, it } from 'node:test';

import { CloseCodes, Connection, ConnectionInterface } from '../src/connection.js';
import { OutgoingStreamController, StreamCloseCodes } from '../src/stream.js';
import { trimSlash, uuid } from '../src/utils/index.js';

function makeWs(readyState: number = 1) {
    const sent: any[] = [];
    const ws: any = {
        readyState,
        send: (data: string) => {
            sent.push(JSON.parse(data));
        },
        close: () => {},
        onopen: null,
        onerror: null,
        onclose: null,
        onmessage: null,
    };
    return { ws, sent };
}

function makeConnection(extra: Partial<ConnectionInterface> = {}): Connection {
    return new Connection(
        {
            onDestroy: () => {},
            onDisconnect: () => {},
            onAccept: () => {},
            ...extra,
        },
        'test',
    );
}

function newStreamMessage(id: string, name: string = 'svc') {
    return { data: JSON.stringify({ type: 'new_stream', id, name, args: null }) };
}

describe('Connection stream request limits', () => {
    it('rejects a new_stream whose id is already waiting in the queue', () => {
        const conn = makeConnection();
        const { ws, sent } = makeWs();
        conn.connect(ws);

        ws.onmessage(newStreamMessage('dup-1'));
        ws.onmessage(newStreamMessage('dup-1'));

        assert.deepStrictEqual(sent, [{ type: 'close_stream', id: 'dup-1', code: StreamCloseCodes.Duplicate }]);
        conn.destroy(CloseCodes.Intentional);
    });

    it('closes new_stream requests beyond maxPendingStreams with Error', () => {
        const conn = makeConnection({ maxPendingStreams: 2 });
        const { ws, sent } = makeWs();
        conn.connect(ws);

        ws.onmessage(newStreamMessage('s1'));
        ws.onmessage(newStreamMessage('s2', 'other'));
        ws.onmessage(newStreamMessage('s3'));

        assert.deepStrictEqual(sent, [{ type: 'close_stream', id: 's3', code: StreamCloseCodes.Error }]);
        conn.destroy(CloseCodes.Intentional);
    });

    it('uses streamRequestHp as the lifetime of an unacquired stream request', () => {
        const conn = makeConnection({ streamRequestHp: 2 });
        const { ws, sent } = makeWs();
        conn.connect(ws);

        ws.onmessage(newStreamMessage('slow-1'));
        (conn as any).tic();
        assert.strictEqual(sent.length, 0);
        (conn as any).tic();
        assert.deepStrictEqual(sent, [{ type: 'close_stream', id: 'slow-1', code: StreamCloseCodes.Lost }]);
        conn.destroy(CloseCodes.Intentional);
    });

    it('sends on sockets that do not expose readyState', () => {
        const conn = makeConnection();
        const { ws, sent } = makeWs();
        delete ws.readyState;
        conn.connect(ws);

        const res = conn.writeData('stream-1', 'payload');
        assert.strictEqual(res.isOk(), true);
        assert.deepStrictEqual(sent, [{ type: 'data_stream', id: 'stream-1', data: 'payload' }]);
        conn.destroy(CloseCodes.Intentional);
    });
});

describe('Outgoing stream initiation', () => {
    it('retries new_stream on tic when it could not be sent while disconnected', async () => {
        const conn = makeConnection();
        const initPromise = conn.newOutgoingStream('svc', { a: 1 });

        const { ws, sent } = makeWs();
        conn.connect(ws);
        assert.strictEqual(sent.length, 0, 'nothing is sent until the next tic');

        (conn as any).tic();
        assert.strictEqual(sent.length, 1);
        assert.strictEqual(sent[0].type, 'new_stream');
        assert.strictEqual(sent[0].name, 'svc');

        (conn as any).tic();
        assert.strictEqual(sent.length, 1, 'new_stream is sent only once');

        ws.onmessage({ data: JSON.stringify({ type: 'accept_stream', id: sent[0].id }) });
        const result = await initPromise;
        assert.strictEqual(result.isOk(), true);
        conn.destroy(CloseCodes.Intentional);
    });

    it('does not freeze the args object owned by the caller', () => {
        const conn = makeConnection();
        const args = { channel: 'room-1' };
        const controller = new OutgoingStreamController(conn, 'svc', args);

        assert.strictEqual(controller.stream.args, args);
        assert.strictEqual(Object.isFrozen(args), false);
        conn.destroy(CloseCodes.Intentional);
    });
});

describe('Utils fallbacks', () => {
    it('uuid() falls back to getRandomValues when randomUUID is unavailable', () => {
        Object.defineProperty(crypto, 'randomUUID', { value: undefined, configurable: true });
        try {
            const id = uuid();
            assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
            assert.notStrictEqual(uuid(), id);
        } finally {
            delete (crypto as any).randomUUID;
        }
        assert.strictEqual(typeof crypto.randomUUID, 'function');
    });

    it('trimSlash() removes repeated leading and trailing slashes', () => {
        assert.strictEqual(trimSlash('//api/v1//'), 'api/v1');
        assert.strictEqual(trimSlash('///'), '');
    });
});

describe('Connection backpressure (ws.bufferedAmount)', () => {
    it('returns Err without calling ws.send when ws.bufferedAmount + payload exceeds maxBufferedBytes', () => {
        const conn = makeConnection({ maxBufferedBytes: 100 });
        const { ws, sent } = makeWs();
        ws.bufferedAmount = 80;
        conn.connect(ws);

        // Frame JSON is > 20 bytes, so 80 + byteLength > 100
        const res = conn.writeData('stream-1', 'hello world');
        assert.strictEqual(res.isErr(), true);
        assert.strictEqual(sent.length, 0);
        conn.destroy(CloseCodes.Intentional);
    });

    it('accounts for UTF-8 byte length rather than UTF-16 string length', () => {
        const payload = '🚀'.repeat(10); // 10 emoji = 40 UTF-8 bytes (20 UTF-16 code units)
        const frameBytes = Buffer.byteLength(JSON.stringify({ type: 'data_stream', id: 's', data: payload }), 'utf8');
        const conn = makeConnection({ maxBufferedBytes: frameBytes - 1 });
        const { ws, sent } = makeWs();
        ws.bufferedAmount = 0;
        conn.connect(ws);

        const rejected = conn.writeData('s', payload);
        assert.strictEqual(rejected.isErr(), true);
        assert.strictEqual(sent.length, 0);

        const connExact = makeConnection({ maxBufferedBytes: frameBytes });
        const exactWs = makeWs();
        exactWs.ws.bufferedAmount = 0;
        connExact.connect(exactWs.ws);

        const accepted = connExact.writeData('s', payload);
        assert.strictEqual(accepted.isOk(), true);
        assert.strictEqual(exactWs.sent.length, 1);

        conn.destroy(CloseCodes.Intentional);
        connExact.destroy(CloseCodes.Intentional);
    });

    it('recovers and sends once ws.bufferedAmount drains back below threshold', () => {
        const conn = makeConnection({ maxBufferedBytes: 100, backpressureHp: 5 });
        const { ws, sent } = makeWs();
        let closed = false;
        ws.close = () => {
            closed = true;
        };
        ws.bufferedAmount = 95;
        conn.connect(ws);

        assert.strictEqual(conn.writeData('stream-1', 'blocked').isErr(), true);
        assert.strictEqual(sent.length, 0);

        // Sustained for a few ticks (below backpressureHp)
        (conn as any).tic();
        (conn as any).tic();
        assert.strictEqual(closed, false);

        // Buffer drains back below threshold
        ws.bufferedAmount = 0;
        const okRes = conn.writeData('stream-1', 'recovered');
        assert.strictEqual(okRes.isOk(), true);
        assert.deepStrictEqual(sent, [{ type: 'data_stream', id: 'stream-1', data: 'recovered' }]);

        // Subsequent ticks do not close the recovered socket
        for (let i = 0; i < 6; i++) {
            (conn as any).tic();
        }
        assert.strictEqual(closed, false);
        conn.destroy(CloseCodes.Intentional);
    });

    it('enforces the 4 MiB default limit and allows disabling backpressure with maxBufferedBytes: 0', () => {
        const defaultConn = makeConnection();
        const ws1 = makeWs();
        ws1.ws.bufferedAmount = 4 * 1024 * 1024;
        defaultConn.connect(ws1.ws);

        assert.strictEqual(defaultConn.writeData('s1', 'x').isErr(), true);
        assert.strictEqual(ws1.sent.length, 0);
        defaultConn.destroy(CloseCodes.Intentional);

        const disabledConn = makeConnection({ maxBufferedBytes: 0 });
        const ws2 = makeWs();
        ws2.ws.bufferedAmount = 16 * 1024 * 1024;
        disabledConn.connect(ws2.ws);

        assert.strictEqual(disabledConn.writeData('s1', 'x').isOk(), true);
        assert.strictEqual(ws2.sent.length, 1);
        disabledConn.destroy(CloseCodes.Intentional);
    });

    it('terminates a stalled socket when backpressure persists past backpressureHp ticks', () => {
        const conn = makeConnection({ maxBufferedBytes: 100, backpressureHp: 3 });
        const { ws, sent } = makeWs();
        let terminated = 0;
        let closed = 0;
        ws.bufferedAmount = 150;
        ws.terminate = () => {
            terminated++;
            ws.onclose?.({ code: 1006 });
        };
        ws.close = () => {
            closed++;
        };
        conn.connect(ws);

        assert.strictEqual(conn.writeData('s1', 'stall').isErr(), true);
        assert.strictEqual(sent.length, 0);

        (conn as any).tic();
        (conn as any).tic();
        assert.strictEqual(terminated, 0);
        assert.strictEqual(closed, 0);

        // 3rd tick reaches backpressureHp -> terminates stalled socket
        (conn as any).tic();
        assert.strictEqual(terminated, 1);
        assert.strictEqual(closed, 0);
        assert.strictEqual(conn.ws, undefined);
        conn.destroy(CloseCodes.Intentional);
    });

    it('falls back to ws.close() when ws.terminate is absent and resets counter on pong_stream', () => {
        const conn = makeConnection({ maxBufferedBytes: 100, backpressureHp: 3 });
        const { ws } = makeWs();
        let closeCalls = 0;
        ws.bufferedAmount = 150;
        ws.close = () => {
            closeCalls++;
            ws.onclose?.({ code: 1006 });
        };
        conn.connect(ws);

        assert.strictEqual(conn.writeData('s1', 'stall').isErr(), true);
        (conn as any).tic();
        (conn as any).tic();
        assert.strictEqual(closeCalls, 0);

        // Receiving a pong_stream resets the sustained backpressure counter
        ws.onmessage({ data: JSON.stringify({ type: 'pong_stream', id: 'unknown-or-live' }) });
        (conn as any).tic();
        (conn as any).tic();
        assert.strictEqual(closeCalls, 0);

        // 3rd consecutive tick after pong closes the stalled socket
        (conn as any).tic();
        assert.strictEqual(closeCalls, 1);
        conn.destroy(CloseCodes.Intentional);
    });
});
