import assert from 'node:assert';
import { getEventListeners } from 'node:events';
import { describe, it } from 'node:test';

import { CloseCodes, Connection } from '../src/connection/index.js';
import { ConnectionStatus, Socket, SocketErr } from '../src/socket/index.js';

describe('Socket and Connection Lifecycle', () => {
    it('initializes socket and checks connection status accurately', () => {
        const mockDriver = {
            onDestroy: () => {},
            onDisconnect: () => {},
            onAccept: () => {},
        };
        const conn = new Connection(mockDriver, 'session-abc');
        const socket = new Socket(conn);

        assert.strictEqual(socket.arg, 'session-abc');
        assert.strictEqual(socket.isDead(), false);
        assert.strictEqual(socket.getConnectionStatus(), ConnectionStatus.Unknown);

        const mockWs = {
            readyState: 1, // Open
            send: () => {},
            close: () => {},
            onopen: null,
            onerror: null,
            onclose: null,
            onmessage: null,
        };

        conn.connect(mockWs as any);
        assert.strictEqual(socket.getConnectionStatus(), ConnectionStatus.Open);

        mockWs.readyState = 2; // Closing
        assert.strictEqual(socket.getConnectionStatus(), ConnectionStatus.Closing);

        mockWs.readyState = 3; // Closed
        assert.strictEqual(socket.getConnectionStatus(), ConnectionStatus.Closed);

        mockWs.readyState = 0; // Connecting
        assert.strictEqual(socket.getConnectionStatus(), ConnectionStatus.Connecting);

        socket.destroy();
        assert.strictEqual(socket.isDead(), true);
    });

    it('handles streams over connection properly', async () => {
        let destroyed = false;
        const mockDriver = {
            onDestroy: () => {
                destroyed = true;
            },
            onDisconnect: () => {},
            onAccept: () => {},
        };
        const conn = new Connection(mockDriver, 'test-stream-arg');
        const socket = new Socket(conn);

        // Acquiring incoming stream while pending
        const acquirePromise = socket.incomingStream('dataChannel');
        assert.ok(acquirePromise instanceof Promise);

        socket.destroy();
        assert.strictEqual(destroyed, true);
    });

    it('does not send or clear ws when readyState is not open (readyState !== 1)', () => {
        let sendCalled = false;
        const mockWs = {
            readyState: 0, // Connecting
            send: () => {
                sendCalled = true;
            },
            close: () => {},
            onopen: null,
            onerror: null,
            onclose: null,
            onmessage: null,
        };
        const mockDriver = {
            onDestroy: () => {},
            onDisconnect: () => {},
            onAccept: () => {},
        };
        const conn = new Connection(mockDriver, 'test');
        conn.connect(mockWs as any);

        const res = conn.writeData('stream-1', 'payload');
        assert.strictEqual(res.isErr(), true);
        assert.strictEqual(sendCalled, false);
        assert.strictEqual(conn.ws, mockWs);
    });

    it('handles JSON.stringify failures gracefully without clearing ws or closing', () => {
        let sendCalled = false;
        let closeCalled = false;
        const mockWs = {
            readyState: 1, // Open
            send: () => {
                sendCalled = true;
            },
            close: () => {
                closeCalled = true;
            },
            onopen: null,
            onerror: null,
            onclose: null,
            onmessage: null,
        };
        const mockDriver = {
            onDestroy: () => {},
            onDisconnect: () => {},
            onAccept: () => {},
        };
        const conn = new Connection(mockDriver, 'test');
        conn.connect(mockWs as any);

        const circularObj: any = {};
        circularObj.self = circularObj;

        const res = conn.writeData('stream-1', circularObj);
        assert.strictEqual(res.isErr(), true);
        assert.strictEqual(sendCalled, false);
        assert.strictEqual(closeCalled, false);
        assert.strictEqual(conn.ws, mockWs);
    });

    it('calls ws.close() with no code and allows onclose to run onDisconnect when send throws', () => {
        let closeArg: any = 'NOT_CALLED';
        let disconnectedCode: number | undefined;

        const mockWs: any = {
            readyState: 1,
            send: () => {
                throw new Error('Network send error');
            },
            close: (code?: number) => {
                closeArg = code;
                if (mockWs.onclose) {
                    mockWs.onclose({ code: code ?? 1006 });
                }
            },
            onopen: null,
            onerror: null,
            onclose: null,
            onmessage: null,
        };
        const mockDriver = {
            onDestroy: () => {},
            onDisconnect: (code: number) => {
                disconnectedCode = code;
            },
            onAccept: () => {},
        };
        const conn = new Connection(mockDriver, 'test');
        conn.connect(mockWs);

        const res = conn.writeData('stream-1', 'payload');
        assert.strictEqual(res.isErr(), true);
        assert.strictEqual(closeArg, undefined); // called with NO code
        assert.strictEqual(disconnectedCode, 1006); // onDisconnect successfully fired
        assert.strictEqual(conn.ws, undefined); // cleaned up by onclose
    });

    it('flushes queued stream requests via roll() when ws.onopen fires', async () => {
        const mockWs: any = {
            readyState: 0, // Connecting
            send: (data: string) => {},
            close: () => {},
            onopen: null,
            onerror: null,
            onclose: null,
            onmessage: null,
        };
        const mockDriver = {
            onDestroy: () => {},
            onDisconnect: () => {},
            onAccept: () => {},
        };
        const conn = new Connection(mockDriver, 'test');
        conn.connect(mockWs);

        const acquirePromise = conn.acquireStream('my-service');

        // Message arrives while socket is still CONNECTING
        mockWs.onmessage({
            data: JSON.stringify({
                type: 'new_stream',
                id: 'stream-xyz',
                name: 'my-service',
                args: { foo: 'bar' },
            }),
        });

        // Socket transitions to OPEN and fires onopen
        mockWs.readyState = 1;
        mockWs.onopen({});

        // Queued stream should have been accepted and resolved
        const result = await acquirePromise;
        assert.strictEqual(result.isOk(), true);
        assert.strictEqual(result.unwrap().id, 'stream-xyz');
        conn.destroy(CloseCodes.Intentional);
    });
});

describe('incomingStream and acquireStream cancellation', () => {
    function makeSetup() {
        const sent: any[] = [];
        const ws: any = {
            readyState: 1,
            send: (data: string) => {
                sent.push(JSON.parse(data));
            },
            close: () => {},
            onopen: null,
            onerror: null,
            onclose: null,
            onmessage: null,
        };
        const conn = new Connection(
            {
                onDestroy: () => {},
                onDisconnect: () => {},
                onAccept: () => {},
            },
            'cancel-test',
        );
        conn.connect(ws);
        const socket = new Socket(conn);
        const promises = (conn as any).promises as Map<string, unknown>;
        return { conn, socket, ws, sent, promises };
    }

    it('preserves SocketErr numeric enum values and adds Cancelled', () => {
        assert.strictEqual(SocketErr.Closed, 0);
        assert.strictEqual(SocketErr.Concurrent, 1);
        assert.strictEqual(SocketErr.Cancelled, 2);
    });

    it('cancels a waiting incomingStream via AbortController and removes it from promises', async () => {
        const { socket, promises } = makeSetup();
        const ac = new AbortController();

        const pending = socket.incomingStream('modal-stream', ac.signal);
        assert.strictEqual(promises.has('modal-stream'), true);
        assert.strictEqual(getEventListeners(ac.signal, 'abort').length, 1);

        ac.abort();

        const result = await pending;
        assert.ok(result.isErr());
        assert.strictEqual(result.e, SocketErr.Cancelled);
        assert.strictEqual(promises.has('modal-stream'), false);
        assert.strictEqual(promises.size, 0);
        assert.strictEqual(getEventListeners(ac.signal, 'abort').length, 0);
        assert.strictEqual(socket.isDead(), false);

        socket.destroy();
    });

    it('resolves immediately with Cancelled for an already-aborted signal without leaking or disturbing existing acquirer', async () => {
        const { socket, ws, promises } = makeSetup();
        const ac = new AbortController();
        ac.abort();

        const immediate = await socket.incomingStream('pre-aborted', ac.signal);
        assert.ok(immediate.isErr());
        assert.strictEqual(immediate.e, SocketErr.Cancelled);
        assert.strictEqual(promises.size, 0);
        assert.strictEqual(getEventListeners(ac.signal, 'abort').length, 0);

        // Also verify an already-aborted call does not displace an active waiter for the same name
        const activeWait = socket.incomingStream('shared');
        assert.strictEqual(promises.has('shared'), true);

        const abortedSecond = await socket.incomingStream('shared', ac.signal);
        assert.ok(abortedSecond.isErr());
        assert.strictEqual(abortedSecond.e, SocketErr.Cancelled);
        assert.strictEqual(promises.has('shared'), true);

        ws.onmessage({
            data: JSON.stringify({
                type: 'new_stream',
                id: 'stream-shared-1',
                name: 'shared',
                args: null,
            }),
        });
        const activeRes = await activeWait;
        assert.strictEqual(activeRes.isOk(), true);
        assert.strictEqual(activeRes.unwrap().id, 'stream-shared-1');

        socket.destroy();
    });

    it('replacing a waiting incomingStream and aborting the old signal does not affect the newer acquirer', async () => {
        const { socket, ws, promises } = makeSetup();
        const oldAc = new AbortController();
        const newAc = new AbortController();

        const first = socket.incomingStream('chat', oldAc.signal);
        assert.strictEqual(getEventListeners(oldAc.signal, 'abort').length, 1);

        const second = socket.incomingStream('chat', newAc.signal);
        const firstRes = await first;
        assert.ok(firstRes.isErr());
        assert.strictEqual(firstRes.e, SocketErr.Concurrent);
        // Old abort listener was cleaned up on Concurrent replacement
        assert.strictEqual(getEventListeners(oldAc.signal, 'abort').length, 0);
        assert.strictEqual(getEventListeners(newAc.signal, 'abort').length, 1);

        // Aborting the old signal must not delete or cancel the newer acquirer
        oldAc.abort();
        assert.strictEqual(promises.has('chat'), true);

        ws.onmessage({
            data: JSON.stringify({
                type: 'new_stream',
                id: 'stream-chat-2',
                name: 'chat',
                args: { room: 'general' },
            }),
        });

        const secondRes = await second;
        assert.strictEqual(secondRes.isOk(), true);
        assert.strictEqual(secondRes.unwrap().id, 'stream-chat-2');
        assert.strictEqual(promises.has('chat'), false);
        assert.strictEqual(getEventListeners(newAc.signal, 'abort').length, 0);

        socket.destroy();
    });

    it('cleans up abort listener on normal stream acquisition (both waiting and already-queued)', async () => {
        const { socket, ws, promises } = makeSetup();

        // Case 1: waits then receives stream
        const ac1 = new AbortController();
        const waiting = socket.incomingStream('events', ac1.signal);
        assert.strictEqual(getEventListeners(ac1.signal, 'abort').length, 1);

        ws.onmessage({
            data: JSON.stringify({
                type: 'new_stream',
                id: 'stream-ev-1',
                name: 'events',
                args: { ok: true },
            }),
        });

        const res1 = await waiting;
        assert.strictEqual(res1.isOk(), true);
        assert.strictEqual(res1.unwrap().id, 'stream-ev-1');
        assert.strictEqual(promises.has('events'), false);
        assert.strictEqual(getEventListeners(ac1.signal, 'abort').length, 0);

        // Case 2: stream already queued before incomingStream is called
        const ac2 = new AbortController();
        ws.onmessage({
            data: JSON.stringify({
                type: 'new_stream',
                id: 'stream-ev-2',
                name: 'events',
                args: { queued: true },
            }),
        });

        const res2 = await socket.incomingStream('events', ac2.signal);
        assert.strictEqual(res2.isOk(), true);
        assert.strictEqual(res2.unwrap().id, 'stream-ev-2');
        assert.strictEqual(promises.has('events'), false);
        assert.strictEqual(getEventListeners(ac2.signal, 'abort').length, 0);

        socket.destroy();
    });

    it('cleans up abort listener when socket is destroyed (Closed)', async () => {
        const { socket, promises } = makeSetup();
        const ac = new AbortController();

        const pending = socket.incomingStream('closing-stream', ac.signal);
        assert.strictEqual(getEventListeners(ac.signal, 'abort').length, 1);

        socket.destroy();

        const res = await pending;
        assert.ok(res.isErr());
        assert.strictEqual(res.e, SocketErr.Closed);
        assert.strictEqual(promises.size, 0);
        assert.strictEqual(getEventListeners(ac.signal, 'abort').length, 0);
    });

    it('supports Socket.cancelIncomingStream and Connection.cancelAcquireStream helpers', async () => {
        const { socket, promises } = makeSetup();
        const ac = new AbortController();

        assert.strictEqual(socket.cancelIncomingStream('missing'), false);

        const pending = socket.incomingStream('manual-cancel', ac.signal);
        assert.strictEqual(promises.has('manual-cancel'), true);
        assert.strictEqual(getEventListeners(ac.signal, 'abort').length, 1);

        assert.strictEqual(socket.cancelIncomingStream('manual-cancel'), true);
        assert.strictEqual(socket.cancelIncomingStream('manual-cancel'), false);

        const res = await pending;
        assert.ok(res.isErr());
        assert.strictEqual(res.e, SocketErr.Cancelled);
        assert.strictEqual(promises.has('manual-cancel'), false);
        assert.strictEqual(getEventListeners(ac.signal, 'abort').length, 0);

        // Aborting after manual cancellation is a harmless no-op
        ac.abort();

        socket.destroy();
    });
});
