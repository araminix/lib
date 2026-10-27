import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { describe, it } from 'node:test';

import { LOG_LEVEL, Logger } from '../src/utils/logger.js';

function captureConsole(run: () => void): { log: string[]; error: string[] } {
    const captured = { log: [] as string[], error: [] as string[] };
    const originalLog = console.log;
    const originalError = console.error;
    console.log = (...args: unknown[]) => captured.log.push(args.map(String).join(' '));
    console.error = (...args: unknown[]) => captured.error.push(args.map(String).join(' '));
    try {
        run();
    } finally {
        console.log = originalLog;
        console.error = originalError;
    }
    return captured;
}

describe('Logger levels and output', () => {
    it('prints info at the default DEBUG level and hides debug at INFO level', () => {
        const atDebug = captureConsole(() => new Logger().info('visible-info'));
        assert.ok(atDebug.log.some((line) => line.includes('[INFO]')));

        const atInfo = captureConsole(() => {
            const l = new Logger();
            l.setLevel(LOG_LEVEL.INFO);
            l.debug('hidden-debug');
            l.trace('hidden-trace');
            l.info('shown-info');
        });
        assert.strictEqual(atInfo.log.length, 1);
        assert.ok(atInfo.log[0].includes('[INFO]'));
    });

    it('serialises circular objects and errors in json format without throwing', () => {
        const circular: any = { name: 'loop' };
        circular.self = circular;
        const out = captureConsole(() => {
            const l = new Logger({ level: LOG_LEVEL.TRACE, showStackTrace: false, showTimestamp: false, format: 'json' });
            l.error('failed:', new Error('boom'), circular, { big: 10n });
        });
        assert.strictEqual(out.error.length, 1);
        const entry = JSON.parse(out.error[0]);
        assert.strictEqual(entry.level, 'ERROR');
        assert.ok(entry.message.includes('Error: boom'));
        assert.ok(entry.message.includes('[Circular]'));
        assert.ok(entry.message.includes('"big":"10"'));
    });

    it('appends stored lines to <filePrefix>-logs-<date>.log without blocking', async () => {
        const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'araminix-logger-'));
        try {
            const l = new Logger({ level: LOG_LEVEL.ERROR, showStackTrace: false, showTimestamp: false, format: 'text' });
            l.initStore({ directory, level: LOG_LEVEL.INFO, filePrefix: 'test' });
            l.info('first line');
            l.warn('second line', { a: 1 });
            l.debug('not stored');

            await (l as any).storeChain;
            const streams = [...(l as any).storeStreams.values()];
            await Promise.all(streams.map((stream: fs.WriteStream) => new Promise((resolve) => stream.end(resolve))));

            const file = path.join(directory, `test-logs-${Logger.getTodaysDate()}.log`);
            const lines = fs.readFileSync(file, 'utf8').trim().split('\n');
            assert.strictEqual(lines.length, 2);
            assert.ok(lines[0].endsWith('[INFO] first line'));
            assert.ok(lines[1].endsWith('[WARN] second line {"a":1}'));
        } finally {
            fs.rmSync(directory, { recursive: true, force: true });
        }
    });
});
