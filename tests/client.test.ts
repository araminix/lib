import assert from 'node:assert';
import { afterEach, beforeEach, describe, it } from 'node:test';
import WS from 'websocket';

import { Client } from '../src/client.js';

describe('Client Session Security and Headers', () => {
    let capturedUrl: string | undefined;
    let capturedProtocols: any;
    let capturedOrigin: any;
    let capturedHeaders: Record<string, string> | undefined;
    const originalW3C = (WS as any).w3cwebsocket;

    beforeEach(() => {
        capturedUrl = undefined;
        capturedProtocols = undefined;
        capturedOrigin = undefined;
        capturedHeaders = undefined;
        (WS as any).w3cwebsocket = function (url: string, protocols: any, origin: any, headers: any) {
            capturedUrl = url;
            capturedProtocols = protocols;
            capturedOrigin = origin;
            capturedHeaders = headers;
            return {
                readyState: 0,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
        };
    });

    afterEach(() => {
        (WS as any).w3cwebsocket = originalW3C;
    });

    it('connects to /ws/:endpoint with no headers when token is omitted (browser mode)', async () => {
        const client = new Client({
            host: 'http://127.0.0.1:3000',
            wsHost: 'ws://127.0.0.1:3000',
        });

        const connectPromise = client.connect('platform', 'user-session-1');
        assert.ok(connectPromise instanceof Promise);

        assert.strictEqual(capturedUrl, 'ws://127.0.0.1:3000/ws/platform/user-session-1');
        assert.strictEqual(capturedHeaders, undefined);
    });

    it('connects to /ws/:endpoint with x-token header when token is supplied (Node agent mode)', async () => {
        const client = new Client({
            host: 'http://127.0.0.1:3000',
            wsHost: 'ws://127.0.0.1:3000',
            token: 'agent-secret-token-xyz',
        });

        const connectPromise = client.connect('platform');
        assert.ok(connectPromise instanceof Promise);

        // Crucial security check: token must NOT be in URL path
        assert.strictEqual(capturedUrl, 'ws://127.0.0.1:3000/ws/platform');
        assert.ok(!capturedUrl.includes('agent-secret-token-xyz'));
        assert.ok(!capturedUrl.includes('ws-token'));

        // Crucial security check: token MUST be sent in x-token header
        assert.deepStrictEqual(capturedHeaders, { 'x-token': 'agent-secret-token-xyz' });
    });

    it('respects mount prefix in WebSocket URL without token in path', async () => {
        const client = new Client({
            host: 'http://127.0.0.1:3000',
            wsHost: 'ws://127.0.0.1:3000',
            mount: '/api/v1',
            token: 'custom-token',
        });

        client.connect('agent');
        assert.strictEqual(capturedUrl, 'ws://127.0.0.1:3000/api/v1/ws/agent');
        assert.deepStrictEqual(capturedHeaders, { 'x-token': 'custom-token' });
    });
});
