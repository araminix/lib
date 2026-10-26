import assert from 'node:assert';
import { describe, it } from 'node:test';

import { LOG_LEVEL, Logger } from '../src/utils/logger.js';

describe('Logger', () => {
    it('instantiates with custom configuration', () => {
        const l = new Logger({
            level: LOG_LEVEL.INFO,
            showStackTrace: false,
            showTimestamp: false,
            format: 'json',
            prefix: 'TEST',
        });
        assert.strictEqual(typeof l.info, 'function');
        assert.strictEqual(typeof l.error, 'function');
    });

    it('formats timestamp and date', () => {
        const ts = Logger.getTimestamp();
        assert.strictEqual(typeof ts, 'string');
        assert.ok(ts.length > 5);

        const date = Logger.getTodaysDate();
        assert.match(date, /^\d{4}-\d{2}-\d{2}$/);
    });

    it('updates level dynamically', () => {
        const l = new Logger();
        l.setLevel(LOG_LEVEL.ERROR);
        // Shouldn't throw
        l.info('Should not print');
        l.error('Error test');
    });
});
