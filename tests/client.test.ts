import axios from 'axios';
import assert from 'node:assert';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { Client, ConnectFail } from '../src/client.js';
import { WsCloseCodes } from '../src/connection.js';

describe('Client Session Security and Headers', () => {
    let capturedUrl: string | undefined;
    let capturedHeaders: Record<string, string> | undefined;
    const originalWebSocket = (globalThis as any).WebSocket;

    beforeEach(() => {
        capturedUrl = undefined;
        capturedHeaders = undefined;
        (globalThis as any).WebSocket = function (url: string, init?: { headers?: Record<string, string> }) {
            capturedUrl = url;
            capturedHeaders = init?.headers;
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
        (globalThis as any).WebSocket = originalWebSocket;
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

    it('sends DELETE /2fa/disable without body when code is omitted', async () => {
        const originalDelete = axios.delete;
        let deleteCall: any;
        (axios as any).delete = async (url: string, config: any) => {
            deleteCall = { url, config };
            return { data: { success: true } };
        };
        try {
            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
                token: 'user-token',
            });
            const res = await client.disable2FA();
            assert.ok(res.isOk());
            assert.strictEqual(deleteCall.url, 'http://127.0.0.1:3000/2fa/disable');
            assert.strictEqual(deleteCall.config.data, undefined);
            assert.deepStrictEqual(deleteCall.config.headers, { 'X-Requested-With': 'XMLHttpRequest', 'x-token': 'user-token' });
        } finally {
            axios.delete = originalDelete;
        }
    });

    it('sends DELETE /2fa/disable with code in body when code is provided', async () => {
        const originalDelete = axios.delete;
        let deleteCall: any;
        (axios as any).delete = async (url: string, config: any) => {
            deleteCall = { url, config };
            return { data: { success: true } };
        };
        try {
            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
                token: 'user-token',
            });
            const res = await client.disable2FA('123456');
            assert.ok(res.isOk());
            assert.strictEqual(deleteCall.url, 'http://127.0.0.1:3000/2fa/disable');
            assert.deepStrictEqual(deleteCall.config.data, { code: '123456' });
            assert.deepStrictEqual(deleteCall.config.headers, { 'X-Requested-With': 'XMLHttpRequest', 'x-token': 'user-token' });
        } finally {
            axios.delete = originalDelete;
        }
    });

    it('sends X-Requested-With without x-token on cookie-authenticated (browser) requests', async () => {
        const originalPost = axios.post;
        const postCalls: Array<{ url: string; config: any }> = [];
        (axios as any).post = async (url: string, _data: any, config: any) => {
            postCalls.push({ url, config });
            return { data: { id: 'f1', name: 'a.txt', size: 1, mimetype: 'text/plain', userId: 'u1' } };
        };
        try {
            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
            });
            assert.ok((await client.request2FA()).isOk());
            assert.ok((await client.upload('file-content')).isOk());
            assert.strictEqual(postCalls.length, 2);
            for (const call of postCalls) {
                assert.deepStrictEqual(call.config.headers, { 'X-Requested-With': 'XMLHttpRequest' });
                assert.strictEqual(call.config.withCredentials, true);
            }
        } finally {
            axios.post = originalPost;
        }
    });
});

describe('Client Connection Lifecycle, Timeouts, and Backoff', () => {
    const originalWebSocket = (globalThis as any).WebSocket;

    afterEach(() => {
        (globalThis as any).WebSocket = originalWebSocket;
    });

    it('resolves connect() with ConnectFail.Concurrent when another connection takes over (4409)', async () => {
        let mockWs: any;
        (globalThis as any).WebSocket = function () {
            mockWs = {
                readyState: 0,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
            return mockWs;
        };

        const client = new Client({
            host: 'http://127.0.0.1:3000',
            wsHost: 'ws://127.0.0.1:3000',
        });

        const connectPromise = client.connect('agent');
        mockWs.onclose({ code: WsCloseCodes.ConcurrentConnection });
        const res = await connectPromise;
        assert.ok(res.isErr());
        if (res.isErr()) {
            assert.strictEqual(res.e, ConnectFail.Concurrent);
        }
    });

    it('aborts connection attempt and resolves with ConnectFail.Unexpected when connectTimeout expires', async () => {
        (globalThis as any).WebSocket = function () {
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

        const client = new Client({
            host: 'http://127.0.0.1:3000',
            wsHost: 'ws://127.0.0.1:3000',
            connectTimeout: 50,
        });

        const start = Date.now();
        const res = await client.connect('agent');
        const elapsed = Date.now() - start;

        assert.ok(elapsed >= 40, `Elapsed time ${elapsed}ms should be at least 40ms`);
        assert.ok(res.isErr());
        if (res.isErr()) {
            assert.strictEqual(res.e, ConnectFail.Unexpected);
        }
    });

    it('clears connectTimeout when connection is accepted', async () => {
        let mockWs: any;
        (globalThis as any).WebSocket = function () {
            mockWs = {
                readyState: 1,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
            return mockWs;
        };

        const client = new Client({
            host: 'http://127.0.0.1:3000',
            wsHost: 'ws://127.0.0.1:3000',
            connectTimeout: 50,
        });

        const connectPromise = client.connect('agent');
        mockWs.onopen();
        mockWs.onmessage({ data: JSON.stringify({ type: 'accept_endpoint' }) });

        const res = await connectPromise;
        assert.strictEqual(res.isOk(), true);
        const socket = res.unwrap();
        assert.strictEqual(socket.isDead(), false);

        await new Promise((resolve) => setTimeout(resolve, 70));
        assert.strictEqual(socket.isDead(), false);
        socket.destroy();
    });

    it('calculates exponential backoff delay with jitter (min(30s, 500ms * 2^n) * (0.5..1.5))', async () => {
        const scheduledDelays: number[] = [];
        const originalSetTimeout = globalThis.setTimeout;
        const originalRandom = Math.random;

        let currentWs: any;
        (globalThis as any).WebSocket = function () {
            currentWs = {
                readyState: 0,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
            return currentWs;
        };

        try {
            Math.random = () => 0.5;
            let lastCallback: Function | undefined;

            (globalThis as any).setTimeout = function (cb: Function, delay: number) {
                scheduledDelays.push(delay);
                lastCallback = cb;
                return 123;
            };

            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
            });

            client.connect('agent');

            // Disconnect 1 (attempt 0): 500ms * 2^0 * 1.0 = 500ms
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[0], 500);

            // Run reconnect callback -> new currentWs created
            lastCallback!();
            // Disconnect 2 (attempt 1): 500ms * 2^1 * 1.0 = 1000ms
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[1], 1000);

            // Run reconnect callback -> new currentWs created
            lastCallback!();
            // Disconnect 3 (attempt 2): 500ms * 2^2 * 1.0 = 2000ms
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[2], 2000);

            // Run reconnect callback -> new currentWs created
            lastCallback!();
            // Disconnect 4 (attempt 3): 500ms * 2^3 * 1.0 = 4000ms
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[3], 4000);

            // Run reconnect callback -> new currentWs created
            lastCallback!();
            // Disconnect 5 (attempt 4): 500ms * 2^4 * 1.0 = 8000ms
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[4], 8000);

            // Run reconnect callback -> new currentWs created
            lastCallback!();
            // Disconnect 6 (attempt 5): 500ms * 2^5 * 1.0 = 16000ms
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[5], 16000);

            // Run reconnect callback -> new currentWs created
            lastCallback!();
            // Disconnect 7 (attempt 6): min(30000, 500ms * 2^6) * 1.0 = 30000ms
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[6], 30000);

            // Run reconnect callback -> new currentWs created
            lastCallback!();
            // Disconnect 8 (attempt 7): min(30000, 500ms * 2^7) * 1.0 = 30000ms
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[7], 30000);

            // Test jitter range with random = 0.0 -> jitter = 0.5 (half delay: 30000 * 0.5 = 15000)
            Math.random = () => 0.0;
            lastCallback!();
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[8], 15000);

            // Test jitter range with random = 1.0 -> jitter = 1.5 (1.5x delay: 30000 * 1.5 = 45000)
            Math.random = () => 1.0;
            lastCallback!();
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[9], 45000);
        } finally {
            globalThis.setTimeout = originalSetTimeout;
            Math.random = originalRandom;
        }
    });

    it('keeps growing the backoff when a socket opens and closes immediately', async () => {
        const scheduledDelays: number[] = [];
        const originalSetTimeout = globalThis.setTimeout;
        const originalRandom = Math.random;

        let currentWs: any;
        (globalThis as any).WebSocket = function () {
            currentWs = {
                readyState: 1,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
            return currentWs;
        };

        try {
            Math.random = () => 0.5;
            let lastCallback: Function | undefined;

            (globalThis as any).setTimeout = function (cb: Function, delay: number) {
                scheduledDelays.push(delay);
                lastCallback = cb;
                return 123;
            };

            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
            });

            client.connect('agent');

            // Disconnect 1 -> delay 500
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 500);
            lastCallback!();

            // Disconnect 2 -> delay 1000
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 1000);
            lastCallback!();

            // Server opens the socket and closes it right away (e.g. 1011 during an outage)
            currentWs.onopen();
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 5000, 'opening schedules the stability check');
            currentWs.onclose({ code: WsCloseCodes.InternalError });

            // Backoff must not reset: attempt 2 -> 2000
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 2000);
        } finally {
            globalThis.setTimeout = originalSetTimeout;
            Math.random = originalRandom;
        }
    });

    it('resets exponential backoff once a reopened socket stays open for the stable period', async () => {
        const scheduledDelays: number[] = [];
        const originalSetTimeout = globalThis.setTimeout;
        const originalRandom = Math.random;

        let currentWs: any;
        (globalThis as any).WebSocket = function () {
            currentWs = {
                readyState: 1,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
            return currentWs;
        };

        try {
            Math.random = () => 0.5;
            let lastCallback: Function | undefined;

            (globalThis as any).setTimeout = function (cb: Function, delay: number) {
                scheduledDelays.push(delay);
                lastCallback = cb;
                return 123;
            };

            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
            });

            client.connect('agent');

            // Disconnect 1 -> delay 500
            currentWs.onclose({ code: 1006 });
            lastCallback!();

            // Disconnect 2 -> delay 1000
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 1000);
            lastCallback!();

            // New ws opens and stays open for the stable period
            currentWs.onopen();
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 5000);
            lastCallback!();

            // Disconnect 3 -> delay should be 500 again
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 500);
        } finally {
            globalThis.setTimeout = originalSetTimeout;
            Math.random = originalRandom;
        }
    });

    it('resets exponential backoff on onAccept', async () => {
        const scheduledDelays: number[] = [];
        const originalSetTimeout = globalThis.setTimeout;
        const originalRandom = Math.random;

        let currentWs: any;
        (globalThis as any).WebSocket = function () {
            currentWs = {
                readyState: 1,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
            return currentWs;
        };

        try {
            Math.random = () => 0.5;
            let reconnectCallback: Function | undefined;

            (globalThis as any).setTimeout = function (cb: Function, delay: number) {
                scheduledDelays.push(delay);
                reconnectCallback = cb;
                return 123;
            };

            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
            });

            const connectPromise = client.connect('agent');

            // Disconnect 1 -> delay 500
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 500);

            // Reconnect 1
            reconnectCallback!();

            // Disconnect 2 -> delay 1000
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 1000);

            // Reconnect 2
            reconnectCallback!();

            // Server accepts endpoint
            currentWs.onmessage({ data: JSON.stringify({ type: 'accept_endpoint' }) });
            const res = await connectPromise;
            assert.strictEqual(res.isOk(), true);

            // Now disconnect after being accepted
            currentWs.onclose({ code: 1006 });
            // Delay must reset back to 500ms
            assert.strictEqual(scheduledDelays[scheduledDelays.length - 1], 500);
        } finally {
            globalThis.setTimeout = originalSetTimeout;
            Math.random = originalRandom;
        }
    });

    it('stops reconnecting and destroys connection when maxReconnectAttempts is exceeded', async () => {
        const originalSetTimeout = globalThis.setTimeout;
        let currentWs: any;
        let reconnectCount = 0;

        (globalThis as any).WebSocket = function () {
            reconnectCount++;
            currentWs = {
                readyState: 0,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
            return currentWs;
        };

        try {
            let reconnectCallback: Function | undefined;
            (globalThis as any).setTimeout = function (cb: Function) {
                reconnectCallback = cb;
                return 123;
            };

            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
                maxReconnectAttempts: 2,
            });

            const connectPromise = client.connect('agent');
            assert.strictEqual(reconnectCount, 1);

            // Disconnect 1 -> schedules retry 1
            currentWs.onclose({ code: 1006 });
            assert.ok(reconnectCallback !== undefined);
            const retry1 = reconnectCallback;
            reconnectCallback = undefined;
            retry1();
            assert.strictEqual(reconnectCount, 2);

            // Disconnect 2 -> schedules retry 2
            currentWs.onclose({ code: 1006 });
            assert.ok(reconnectCallback !== undefined);
            const retry2: Function = reconnectCallback;
            reconnectCallback = undefined;
            retry2();
            assert.strictEqual(reconnectCount, 3);

            // Disconnect 3 -> maxReconnectAttempts (2) reached, should not schedule retry 3
            currentWs.onclose({ code: 1006 });
            assert.strictEqual(reconnectCallback, undefined);

            const res = await connectPromise;
            assert.ok(res.isErr());
            if (res.isErr()) {
                assert.strictEqual(res.e, ConnectFail.Unexpected);
            }
        } finally {
            globalThis.setTimeout = originalSetTimeout;
        }
    });

    it('stops immediately on first disconnect when maxReconnectAttempts is 0', async () => {
        let currentWs: any;
        let timeoutScheduled = false;
        const originalSetTimeout = globalThis.setTimeout;

        (globalThis as any).WebSocket = function () {
            currentWs = {
                readyState: 0,
                send: () => {},
                close: () => {},
                onopen: null,
                onerror: null,
                onclose: null,
                onmessage: null,
            };
            return currentWs;
        };

        try {
            (globalThis as any).setTimeout = function () {
                timeoutScheduled = true;
                return 123;
            };

            const client = new Client({
                host: 'http://127.0.0.1:3000',
                wsHost: 'ws://127.0.0.1:3000',
                maxReconnectAttempts: 0,
            });

            const connectPromise = client.connect('agent');
            currentWs.onclose({ code: 1006 });

            assert.strictEqual(timeoutScheduled, false);
            const res = await connectPromise;
            assert.ok(res.isErr());
            if (res.isErr()) {
                assert.strictEqual(res.e, ConnectFail.Unexpected);
            }
        } finally {
            globalThis.setTimeout = originalSetTimeout;
        }
    });

    it('connectAgent propagates ConnectFail.Unexpected on timeout', async () => {
        (globalThis as any).WebSocket = function () {
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

        const client = new Client({
            host: 'http://127.0.0.1:3000',
            wsHost: 'ws://127.0.0.1:3000',
            connectTimeout: 30,
        });

        const res = await client.connectAgent();
        assert.ok(res.isErr());
        if (res.isErr()) {
            assert.strictEqual(res.e, ConnectFail.Unexpected);
        }
    });
});
