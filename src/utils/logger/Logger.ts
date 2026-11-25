import { formatTimestamp, formatTodaysDate, paint, stringifyArg } from './format.js';
import { LogFileStore } from './store.js';
import { DEFAULT_CONFIG, LOG_COLORS, LOG_LEVEL, LOG_PRIORITY, LoggerConfig, StoreOptions } from './types.js';

/**
 * Levelled logger for the console with optional daily log files.
 *
 * - **Console:** `WARN` and `ERROR` go to `console.error`, other levels to `console.log`, as colored text or JSON
 *   ({@link LoggerConfig.format}).
 * - **Files** (Node.js only, see {@link StoreOptions}): lines are appended in order through write streams in the background.
 *   `node:fs` and `node:path` are imported lazily, so browser bundles never need them. File problems never throw; they are
 *   reported with `console.error`.
 *
 * The library logs through the shared {@link logger} instance. Create your own `Logger` for a separate prefix or threshold.
 *
 * @example
 * ```ts
 * import { LOG_LEVEL, Logger } from '@araminix/lib';
 *
 * const log = new Logger({ level: LOG_LEVEL.INFO, showStackTrace: true, showTimestamp: true, format: 'text', prefix: 'billing' });
 * log.info('invoice sent', { id: 42 });
 * log.debug('not printed: below INFO');
 * log.error('payment failed:', new Error('card declined'));
 * ```
 */
export class Logger {
    /** Numeric console threshold, from {@link LoggerConfig.level} or {@link Logger.setLevel}. */
    private logPriority: number;
    /** The current configuration. */
    private config: LoggerConfig = DEFAULT_CONFIG;
    /** Optional daily file storage backend. */
    private readonly fileStore = new LogFileStore();

    /**
     * Creates a logger.
     *
     * @param options - Complete configuration. Defaults to level `DEBUG`, stack traces and timestamps on, `'text'` format, no file
     * storage and no prefix. To enable files, set {@link LoggerConfig.storeOptions} or call {@link Logger.initStore}.
     *
     * @example
     * ```ts
     * const log = new Logger({ level: LOG_LEVEL.DEBUG, showStackTrace: false, showTimestamp: false, format: 'json' });
     * ```
     */
    constructor(options: LoggerConfig = DEFAULT_CONFIG) {
        this.configure(options);
        this.logPriority = LOG_PRIORITY[options.level];
    }

    /** Internal accessors kept for tests that flush pending file writes. */
    private get storeChain(): Promise<void> {
        return this.fileStore.chain;
    }

    private get storeStreams() {
        return this.fileStore.streams;
    }

    /**
     * Merges `options` into the current configuration.
     *
     * Changing `level` updates the console threshold. Passing `storeOptions` (including `null`) makes the next stored line set up
     * the directory again; streams that are already open are replaced only when a different file is needed.
     *
     * @param options - Any subset of {@link LoggerConfig}.
     *
     * @example
     * ```ts
     * logger.configure({ format: 'json', showTimestamp: false });
     * ```
     */
    configure(options: Partial<LoggerConfig>): void {
        this.config = { ...this.config, ...options };
        if (options.level) {
            this.logPriority = LOG_PRIORITY[options.level];
        }
        if (options.storeOptions !== undefined) {
            this.fileStore.reset();
        }
    }

    /** Prints a message if it meets the console threshold and queues it for the log files if it meets the store threshold. */
    private log(level: LOG_LEVEL, args: unknown[]): void {
        const timestamp = Logger.getTimestamp();
        const prefix = this.config.prefix ? ` [${this.config.prefix}]` : '';

        if (LOG_PRIORITY[level] >= this.logPriority) {
            const consoleFn = level === LOG_LEVEL.ERROR || level === LOG_LEVEL.WARN ? console.error : console.log;
            if (this.config.format === 'json') {
                const logObject = {
                    ...(this.config.showTimestamp && { timestamp }),
                    level: level.toUpperCase(),
                    prefix,
                    message: args.map((arg) => stringifyArg(arg, this.config.showStackTrace)).join(' '),
                };
                consoleFn(JSON.stringify(logObject));
            } else {
                const coloredLevel = paint(`[${level.toUpperCase()}]`).color(LOG_COLORS[level]).text;
                const coloredTime = this.config.showTimestamp ? paint(`[${timestamp}]`).color('blue').text + ' ' : '';
                consoleFn(`${coloredTime}${coloredLevel}${prefix}`, ...args);
            }
        }

        if (this.config.storeOptions && LOG_PRIORITY[level] >= LOG_PRIORITY[this.config.storeOptions.level]) {
            const formattedArgs = args.map((arg) => stringifyArg(arg, this.config.showStackTrace)).join(' ');
            this.fileStore.append(this.config.storeOptions, level, `[${timestamp}] [${level.toUpperCase()}] ${formattedArgs}\n`);
        }
    }

    /**
     * Returns the current UTC time in the format used by log output: `YYYY-MM-DD - HH:mm:ss.sss`.
     *
     * @returns For example `'2026-10-27 - 14:03:59.123'`.
     *
     * @example
     * ```ts
     * Logger.getTimestamp(); // '2026-10-27 - 14:03:59.123'
     * ```
     */
    static getTimestamp(): string {
        return formatTimestamp();
    }

    /**
     * Returns the current UTC date as `YYYY-MM-DD`, as used in log file names.
     *
     * @returns For example `'2026-10-27'`.
     *
     * @example
     * ```ts
     * const file = `api-logs-${Logger.getTodaysDate()}.log`;
     * ```
     */
    static getTodaysDate(): string {
        return formatTodaysDate();
    }

    /**
     * Enables file storage (Node.js only) and immediately starts creating the directory.
     *
     * Same as `configure({ storeOptions: options })`, except that the directory is set up right away instead of on the first
     * stored line. Writing happens in the background and never throws; if the directory cannot be created, each stored line
     * reports `Logger store unavailable:` with `console.error`.
     *
     * @param options - Directory, minimum level and optional file prefix; see {@link StoreOptions}.
     *
     * @example
     * ```ts
     * import { LOG_LEVEL, logger } from '@araminix/lib';
     *
     * logger.initStore({ directory: './logs', level: LOG_LEVEL.WARN });
     * logger.warn('disk almost full'); // console + ./logs/folkrum-logs-<date>.log
     * logger.error('disk full'); // console + ./logs/folkrum-errors-<date>.log
     * ```
     */
    initStore(options: StoreOptions): void {
        this.configure({ storeOptions: options });
        this.fileStore.ensureReady(options.directory);
    }

    /**
     * Changes the console threshold. File storage keeps its own threshold ({@link StoreOptions.level}).
     *
     * @param level - The new minimum level for console output.
     *
     * @example
     * ```ts
     * logger.setLevel(LOG_LEVEL.TRACE); // also prints every incoming WebSocket frame
     * ```
     */
    setLevel(level: LOG_LEVEL): void {
        this.logPriority = LOG_PRIORITY[level];
    }

    /**
     * Logs at `ERROR` level (to `console.error`, and to the errors file when storage is enabled).
     *
     * @typeParam T - Tuple type of the arguments.
     * @param args - Values to log. In `'text'` format they are passed to the console unchanged; in JSON output and files each is
     * serialized and joined with spaces.
     *
     * @example
     * ```ts
     * logger.error('request failed:', new Error('timeout'), { url: '/api/items' });
     * ```
     */
    error<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.ERROR, args);
    }

    /**
     * Logs at `WARN` level (to `console.error`).
     *
     * @typeParam T - Tuple type of the arguments.
     * @param args - Values to log, see {@link Logger.error}.
     */
    warn<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.WARN, args);
    }

    /**
     * Logs at `INFO` level (to `console.log`).
     *
     * @typeParam T - Tuple type of the arguments.
     * @param args - Values to log, see {@link Logger.error}.
     */
    info<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.INFO, args);
    }

    /**
     * Logs at `TRACE` level (to `console.log`), the most verbose level.
     *
     * @typeParam T - Tuple type of the arguments.
     * @param args - Values to log, see {@link Logger.error}.
     */
    trace<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.TRACE, args);
    }

    /**
     * Logs at `DEBUG` level (to `console.log`).
     *
     * @typeParam T - Tuple type of the arguments.
     * @param args - Values to log, see {@link Logger.error}.
     */
    debug<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.DEBUG, args);
    }
}

/**
 * The shared default {@link Logger}: level `DEBUG`, `'text'` format, console only.
 *
 * The library writes its own diagnostics through this instance: WebSocket errors and closes, invalid frames, errors caught by
 * `loop()` and `once()`, and every incoming frame at `TRACE`. Configure it to control that output.
 *
 * @example
 * ```ts
 * import { LOG_LEVEL, logger } from '@araminix/lib';
 *
 * logger.configure({ level: LOG_LEVEL.WARN, format: 'json', prefix: 'agent' });
 * logger.initStore({ directory: '/var/log/agent', level: LOG_LEVEL.INFO, filePrefix: 'agent' });
 * ```
 */
export const logger = new Logger();
