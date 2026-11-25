import { COLOR_NAMES } from 'gach';

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
export const LOG_PRIORITY: Record<LOG_LEVEL, number> = {
    [LOG_LEVEL.TRACE]: 0,
    [LOG_LEVEL.DEBUG]: 1,
    [LOG_LEVEL.INFO]: 2,
    [LOG_LEVEL.WARN]: 3,
    [LOG_LEVEL.ERROR]: 4,
};

/** Color (gach color name) of the level tag in `'text'` output. */
export const LOG_COLORS: Record<LOG_LEVEL, string> = {
    [LOG_LEVEL.TRACE]: COLOR_NAMES.LIGHT_MAGENTA,
    [LOG_LEVEL.INFO]: COLOR_NAMES.CYAN,
    [LOG_LEVEL.DEBUG]: COLOR_NAMES.BLUE,
    [LOG_LEVEL.WARN]: COLOR_NAMES.YELLOW,
    [LOG_LEVEL.ERROR]: COLOR_NAMES.RED,
};

/** Default {@link StoreOptions.filePrefix}. */
export const DEFAULT_FILE_PREFIX = 'folkrum';

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
     * Include stack traces when `Error` values are serialized (JSON console output and log files); otherwise they render as
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
     * - `'text'`: a colored `[timestamp] [LEVEL] [prefix]` header followed by the raw arguments (the console formats objects).
     * - `'json'`: one JSON string per message with `timestamp` (if enabled), `level` (upper case), `prefix` (the same
     *   ` [prefix]` text, or `''`) and `message` (all arguments serialized and joined with spaces).
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
export const DEFAULT_CONFIG: LoggerConfig = {
    level: LOG_LEVEL.DEBUG,
    showStackTrace: true,
    showTimestamp: true,
    format: 'text',
    storeOptions: null,
    prefix: null,
};
