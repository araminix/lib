/**
 * Logger module for handling application logging with different levels and formats
 * @module logger
 */
import gach, { COLOR_NAMES } from 'gach';

/**
 * Log levels. Their priority, from most to least verbose, is `TRACE` < `DEBUG` < `INFO` < `WARN` < `ERROR`.
 *
 * A {@link Logger} prints a message when its level is at or above the configured threshold ({@link LoggerConfig.level},
 * `DEBUG` by default). The upper-cased value appears in the output, e.g. `[INFO]`.
 *
 * @example
 * ```ts
 * import { LOG_LEVEL, logger } from '@araminix/lib';
 *
 * logger.setLevel(LOG_LEVEL.WARN); // only WARN and ERROR are printed from now on
 * ```
 */
export enum LOG_LEVEL {
    /** Finest detail, e.g. every incoming WebSocket frame. Priority 0. */
    TRACE = 'trace',
    /** Normal operational messages. Priority 2 (above `DEBUG`, despite the declaration order). */
    INFO = 'info',
    /** Debugging details. Priority 1. */
    DEBUG = 'debug',
    /** Recoverable problems, e.g. a closed socket. Priority 3. Printed with `console.error`. */
    WARN = 'warn',
    /** Failures. Priority 4. Printed with `console.error` and stored in the separate errors file. */
    ERROR = 'error',
}

/** Numeric priority per level; a message is emitted when its priority is at least the threshold's. */
const LOG_PRIORITY: Record<LOG_LEVEL, number> = {
    [LOG_LEVEL.TRACE]: 0,
    [LOG_LEVEL.DEBUG]: 1,
    [LOG_LEVEL.INFO]: 2,
    [LOG_LEVEL.WARN]: 3,
    [LOG_LEVEL.ERROR]: 4,
};

/** Colour (gach colour name) of the level tag in `'text'` output. */
const LOG_COLORS: Record<LOG_LEVEL, string> = {
    [LOG_LEVEL.TRACE]: COLOR_NAMES.LIGHT_MAGENTA,
    [LOG_LEVEL.INFO]: COLOR_NAMES.CYAN,
    [LOG_LEVEL.DEBUG]: COLOR_NAMES.BLUE,
    [LOG_LEVEL.WARN]: COLOR_NAMES.YELLOW,
    [LOG_LEVEL.ERROR]: COLOR_NAMES.RED,
};

/** Wraps text in ANSI colour codes via gach. */
// Node's ESM loader exposes gach's CommonJS exports object (function on `.default`); bundlers often hand over the function itself.
const paint: (text: string) => { color: (name: string) => { text: string } } = (gach as any).default ?? gach;

/** Default {@link StoreOptions.filePrefix}. */
const DEFAULT_FILE_PREFIX = 'folkrum';

/**
 * File storage settings of a {@link Logger} (Node.js only). Pass them to {@link Logger.initStore} or as
 * {@link LoggerConfig.storeOptions}.
 *
 * Two files are written per day: `<filePrefix>-logs-<YYYY-MM-DD>.log` for every stored level except `ERROR`, and
 * `<filePrefix>-errors-<YYYY-MM-DD>.log` for `ERROR`. The date is the UTC date, so files roll over at UTC midnight. Each line
 * reads `[<timestamp>] [<LEVEL>] <message>`, regardless of the console format and prefix.
 *
 * @example
 * ```ts
 * import { LOG_LEVEL, StoreOptions, logger } from '@araminix/lib';
 *
 * const store: StoreOptions = { directory: './logs', level: LOG_LEVEL.INFO, filePrefix: 'api' };
 * logger.initStore(store); // ./logs/api-logs-2026-10-27.log and ./logs/api-errors-2026-10-27.log
 * ```
 */
export interface StoreOptions {
    /** Directory of the log files. Relative paths resolve against `process.cwd()`; missing directories are created recursively. */
    directory: string;
    /** Minimum level written to the files. Independent of the console threshold ({@link LoggerConfig.level}). */
    level: LOG_LEVEL;
    /**
     * Prefix of the log file names.
     *
     * @defaultValue `'folkrum'`
     */
    filePrefix?: string;
}

/**
 * Configuration of a {@link Logger}. The constructor takes a complete configuration; {@link Logger.configure} accepts any subset.
 *
 * @example
 * ```ts
 * import { LOG_LEVEL, Logger, LoggerConfig } from '@araminix/lib';
 *
 * const config: LoggerConfig = { level: LOG_LEVEL.INFO, showStackTrace: false, showTimestamp: true, format: 'json', prefix: 'api' };
 * const log = new Logger(config);
 * ```
 */
export interface LoggerConfig {
    /**
     * Console threshold: messages below this level are not printed. File storage has its own threshold
     * ({@link StoreOptions.level}).
     *
     * @defaultValue `LOG_LEVEL.DEBUG`
     */
    level: LOG_LEVEL;
    /**
     * Include stack traces when `Error` values are serialised (JSON console output and log files); otherwise they render as
     * `Name: message`. In `'text'` console output arguments are passed to the console unchanged, so this has no effect there.
     *
     * @defaultValue `true`
     */
    showStackTrace: boolean;
    /**
     * Start console output with the timestamp (`YYYY-MM-DD - HH:mm:ss.sss`, UTC). Log file lines always include it.
     *
     * @defaultValue `true`
     */
    showTimestamp: boolean;
    /**
     * Console output format.
     * - `'text'`: a coloured `[timestamp] [LEVEL] [prefix]` header followed by the raw arguments (the console formats objects).
     * - `'json'`: one JSON string per message with `timestamp` (if enabled), `level` (upper case), `prefix` (the same
     *   ` [prefix]` text, or `''`) and `message` (all arguments serialised and joined with spaces).
     *
     * Log files always use plain text lines.
     *
     * @defaultValue `'text'`
     */
    format: 'json' | 'text';
    /**
     * File storage settings, or `null`/`undefined` to log to the console only. See also {@link Logger.initStore}.
     *
     * @defaultValue `null`
     */
    storeOptions?: StoreOptions | null;
    /**
     * Label printed as ` [prefix]` after the level in console output. Not written to log files.
     *
     * @defaultValue `null` (no label)
     */
    prefix?: string | null;
}

/** Configuration used when the constructor receives none; also the base that `configure()` merges options onto. */
const DEFAULT_CONFIG: LoggerConfig = {
    level: LOG_LEVEL.DEBUG,
    showStackTrace: true,
    showTimestamp: true,
    format: 'text',
    storeOptions: null,
    prefix: null,
};

/** Type of the lazily imported `node:fs` module. */
type NodeFs = typeof import('node:fs');
/** Type of the lazily imported `node:path` module. */
type NodePath = typeof import('node:path');
/** The Node.js modules needed for file storage. */
type NodeModules = { fs: NodeFs; path: NodePath };

/** Shared promise of the lazily imported Node.js modules; created on first use. */
let nodeModules: Promise<NodeModules> | undefined;

/** Imports `node:fs` and `node:path` once, on first use, so that only file storage depends on them. */
function loadNodeModules(): Promise<NodeModules> {
    if (!nodeModules) {
        // Loaded lazily (and hidden from bundlers) so browser builds never need fs/path; only file storage uses them.
        const fsModule = 'node:fs';
        const pathModule = 'node:path';
        nodeModules = Promise.all([import(/* webpackIgnore: true */ /* @vite-ignore */ fsModule), import(/* webpackIgnore: true */ /* @vite-ignore */ pathModule)]).then(
            ([fs, path]) => ({ fs, path }),
        );
    }
    return nodeModules;
}

/**
 * Serialises one log argument for JSON output and log files: errors as stack (or `Name: message`), objects as JSON that
 * survives circular references, BigInt and nested errors, everything else with `String()`.
 */
function stringifyArg(arg: unknown, showStackTrace: boolean): string {
    if (arg instanceof Error) {
        return showStackTrace && arg.stack ? arg.stack : `${arg.name}: ${arg.message}`;
    }
    if (typeof arg !== 'object' || arg === null) {
        return String(arg);
    }
    try {
        const seen = new WeakSet<object>();
        return JSON.stringify(arg, (_key, value: unknown) => {
            if (value instanceof Error) {
                return { name: value.name, message: value.message, ...(showStackTrace && { stack: value.stack }) };
            }
            if (typeof value === 'bigint') {
                return value.toString();
            }
            if (typeof value === 'object' && value !== null) {
                if (seen.has(value)) {
                    return '[Circular]';
                }
                seen.add(value);
            }
            return value;
        });
    } catch {
        return '[Unserializable Object]';
    }
}

/**
 * Levelled logger for the console with optional daily log files.
 *
 * - **Console:** `WARN` and `ERROR` go to `console.error`, other levels to `console.log`, as coloured text or JSON
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
    /** Resolves with the absolute log directory once it exists; cleared when `storeOptions` change. */
    private storeReady?: Promise<string>;
    /** Chain that serialises file writes so that lines keep their order. */
    private storeChain: Promise<void> = Promise.resolve();
    /** Open append streams by absolute file path. */
    private storeStreams: Map<string, ReturnType<NodeFs['createWriteStream']>> = new Map();

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
            this.storeReady = undefined;
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
            this.store(level, `[${timestamp}] [${level.toUpperCase()}] ${formattedArgs}\n`);
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
        return new Date().toISOString().replace('T', ' - ').replace('Z', '');
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
        return new Date().toISOString().split('T')[0];
    }

    /** File name for today's date: the errors file for `ERROR`, the logs file for every other level. */
    private getLogFileName(level: LOG_LEVEL): string {
        const date = Logger.getTodaysDate();
        const filePrefix = this.config.storeOptions?.filePrefix ?? DEFAULT_FILE_PREFIX;
        return level === LOG_LEVEL.ERROR ? `${filePrefix}-errors-${date}.log` : `${filePrefix}-logs-${date}.log`;
    }

    /** Starts resolving and creating the log directory (once per store configuration) and returns that shared promise. */
    private ensureStore(): Promise<string> {
        if (!this.storeReady) {
            const directory = this.config.storeOptions!.directory;
            this.storeReady = loadNodeModules().then(({ fs, path }) => {
                const absolutePath = path.resolve(directory);
                fs.mkdirSync(absolutePath, { recursive: true });
                return absolutePath;
            });
            // Mark as handled right away; store() reports the failure once it reaches this point in its chain.
            this.storeReady.catch(() => {});
        }
        return this.storeReady;
    }

    /** Appends a line to the right file in the background, in order; failures are reported with `console.error`. */
    private store(level: LOG_LEVEL, line: string): void {
        const fileName = this.getLogFileName(level);
        const ready = this.ensureStore();
        this.storeChain = this.storeChain
            .then(async () => {
                const [directory, { fs, path }] = await Promise.all([ready, loadNodeModules()]);
                const filePath = path.join(directory, fileName);
                let stream = this.storeStreams.get(filePath);
                if (!stream) {
                    // A new file means a new day (or directory): release streams for the old files.
                    // Note: the logs and errors files share this map, so alternating between ERROR and other levels also reopens streams.
                    this.storeStreams.forEach((oldStream) => oldStream.end());
                    this.storeStreams.clear();
                    stream = fs.createWriteStream(filePath, { flags: 'a' });
                    stream.on('error', (error) => console.error('Logger store write failed:', error));
                    this.storeStreams.set(filePath, stream);
                }
                stream.write(line);
            })
            .catch((error: unknown) => {
                console.error('Logger store unavailable:', error);
            });
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
        this.ensureStore();
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
     * serialised and joined with spaces.
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
