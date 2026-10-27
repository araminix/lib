import assert from 'node:assert';
import { describe, it } from 'node:test';

import { CloseCodes, Connection } from '../src/connection.js';
import { ConnectionStatus, Socket } from '../src/Socket.js';

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
    });
});
