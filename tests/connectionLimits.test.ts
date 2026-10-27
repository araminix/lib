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
