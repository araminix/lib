import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, it } from 'node:test';

import { Client } from '../src/client.js';
import { CloseCodes, Connection, WsCloseCodes } from '../src/connection.js';
import { OutgoingStreamController, StreamCloseCodes, StreamErr } from '../src/stream.js';
import { LOG_LEVEL, Logger } from '../src/utils/logger.js';

function makeWs() {
    const sent: any[] = [];
    const ws: any = {
        readyState: 1,
        closedWith: undefined as number | undefined,
        send: (data: string) => sent.push(JSON.parse(data)),
        close: (code?: number) => {
            ws.closedWith = code;
        },
        onopen: null,
        onerror: null,
        onclose: null,
        onmessage: null,
    };
    return { ws, sent };
}

function makeConnection(): Connection {
    return new Connection({ onDestroy: () => {}, onDisconnect: () => {}, onAccept: () => {} }, 'test');
}

describe('Stream writes that cannot be serialised', () => {
    it('reject with StreamErr.Invalid instead of blocking the queue', async () => {
        const conn = makeConnection();
        const controller = new OutgoingStreamController(conn, 'svc', null);
        const circular: any = {};
        circular.self = circular;

        const bad = await controller.stream.write(circular);
        assert.ok(bad.isErr());
        assert.strictEqual(bad.e, StreamErr.Invalid);
        assert.strictEqual(controller.stream.outgoingQueueLength, 0);
        assert.strictEqual(controller.stream.active, true);

        const { ws, sent } = makeWs();
        conn.connect(ws);
        const good = await controller.stream.write({ fine: true });
        assert.ok(good.isOk());
        assert.deepStrictEqual(sent.at(-1), { type: 'data_stream', id: controller.stream.id, data: { fine: true } });
        conn.destroy(CloseCodes.Intentional);
    });
});

describe('Close codes', () => {
    it('reports unknown close_stream codes as Error so waitClose() resolves', async () => {
        const conn = makeConnection();
        const { ws } = makeWs();
        conn.connect(ws);
        const controller = new OutgoingStreamController(conn, 'svc', null);
        (conn as any).streams.set(controller.id, controller);

        ws.onmessage({ data: JSON.stringify({ type: 'close_stream', id: controller.id, code: '' }) });
        assert.strictEqual(await controller.stream.waitClose(), StreamCloseCodes.Error);
        conn.destroy(CloseCodes.Intentional);
    });

    it('closes the socket with 4401 when destroyed as Unauthorized, 4000 otherwise', () => {
        const unauthorized = makeConnection();
        const first = makeWs();
        unauthorized.connect(first.ws);
        unauthorized.destroy(CloseCodes.Unauthorized);
        assert.strictEqual(first.ws.closedWith, WsCloseCodes.Unauthorized);

        const intentional = makeConnection();
        const second = makeWs();
        intentional.connect(second.ws);
        intentional.destroy(CloseCodes.Intentional);
        assert.strictEqual(second.ws.closedWith, WsCloseCodes.Normal);
    });
});

describe('Client URLs and reconnects', () => {
    it('builds WebSocket URLs without double slashes and with encoded name and arg', async () => {
        const originalWebSocket = (globalThis as any).WebSocket;
        const urls: string[] = [];
        (globalThis as any).WebSocket = function (url: string) {
            urls.push(url);
            return { readyState: 0, send: () => {}, close: () => {}, onopen: null, onerror: null, onclose: null, onmessage: null };
        };
        try {
            const client = new Client({ host: 'http://h', wsHost: 'ws://h', mount: '/', connectTimeout: 1 });
            await client.connect('documents', 'a b/c');
            assert.deepStrictEqual(urls, ['ws://h/ws/documents/a%20b%2Fc']);
        } finally {
            (globalThis as any).WebSocket = originalWebSocket;
        }
    });

    it('does not schedule a reconnect when the socket closes after the connection was destroyed', async () => {
        const originalWebSocket = (globalThis as any).WebSocket;
        const originalSetTimeout = globalThis.setTimeout;
        let socket: any;
        (globalThis as any).WebSocket = function () {
            socket = { readyState: 0, send: () => {}, close: () => {}, onopen: null, onerror: null, onclose: null, onmessage: null };
            return socket;
        };
        try {
            const client = new Client({ host: 'http://h', wsHost: 'ws://h', connectTimeout: 1 });
            const result = await client.connect('svc');
            assert.ok(result.isErr());

            const delays: number[] = [];
            (globalThis as any).setTimeout = (callback: () => void, delay: number) => {
                delays.push(delay);
                return originalSetTimeout(callback, delay);
            };
            socket.onclose({ code: 1006 });
            assert.deepStrictEqual(delays, []);
        } finally {
            globalThis.setTimeout = originalSetTimeout;
            (globalThis as any).WebSocket = originalWebSocket;
        }
    });
});

describe('Logger file streams', () => {
    it('keeps the logs and errors files of the day open together', async () => {
        const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'araminix-logger-streams-'));
        try {
            const l = new Logger({ level: LOG_LEVEL.ERROR, showStackTrace: false, showTimestamp: false, format: 'text' });
            const silence = console.error;
            console.error = () => {};
            try {
                l.initStore({ directory, level: LOG_LEVEL.INFO, filePrefix: 'mix' });
                l.info('one');
                l.error('two');
                l.info('three');
                l.error('four');
                await (l as any).storeChain;
            } finally {
                console.error = silence;
            }
            const streams = [...(l as any).storeStreams.values()];
            assert.strictEqual(streams.length, 2, 'both files stay open');
            await Promise.all(streams.map((stream: fs.WriteStream) => new Promise((resolve) => stream.end(resolve))));

            const date = Logger.getTodaysDate();
            const logs = fs
                .readFileSync(path.join(directory, `mix-logs-${date}.log`), 'utf8')
                .trim()
                .split('\n');
            const errors = fs
                .readFileSync(path.join(directory, `mix-errors-${date}.log`), 'utf8')
                .trim()
                .split('\n');
            assert.deepStrictEqual(
                logs.map((line) => line.split('] ').pop()),
                ['one', 'three'],
            );
            assert.deepStrictEqual(
                errors.map((line) => line.split('] ').pop()),
                ['two', 'four'],
            );
        } finally {
            fs.rmSync(directory, { recursive: true, force: true });
        }
    });
});
