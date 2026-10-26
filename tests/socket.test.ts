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
});
