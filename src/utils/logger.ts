/**
 * Logger module for handling application logging with different levels and formats
 * @module logger
 */
import gach, { COLOR_NAMES } from 'gach';

export enum LOG_LEVEL {
    TRACE = 'trace',
    INFO = 'info',
    DEBUG = 'debug',
    WARN = 'warn',
    ERROR = 'error',
}

const LOG_PRIORITY: Record<LOG_LEVEL, number> = {
    [LOG_LEVEL.TRACE]: 0,
    [LOG_LEVEL.DEBUG]: 1,
    [LOG_LEVEL.INFO]: 2,
    [LOG_LEVEL.WARN]: 3,
    [LOG_LEVEL.ERROR]: 4,
};

const LOG_COLORS: Record<LOG_LEVEL, string> = {
    [LOG_LEVEL.TRACE]: COLOR_NAMES.LIGHT_MAGENTA,
    [LOG_LEVEL.INFO]: COLOR_NAMES.CYAN,
    [LOG_LEVEL.DEBUG]: COLOR_NAMES.BLUE,
    [LOG_LEVEL.WARN]: COLOR_NAMES.YELLOW,
    [LOG_LEVEL.ERROR]: COLOR_NAMES.RED,
};

// Node's ESM loader exposes gach's CommonJS exports object (function on `.default`); bundlers often hand over the function itself.
const paint: (text: string) => { color: (name: string) => { text: string } } = (gach as any).default ?? gach;

const DEFAULT_FILE_PREFIX = 'folkrum';

export interface StoreOptions {
    directory: string;
    level: LOG_LEVEL;
    filePrefix?: string;
}

export interface LoggerConfig {
    level: LOG_LEVEL;
    showStackTrace: boolean;
    showTimestamp: boolean;
    format: 'json' | 'text';
    storeOptions?: StoreOptions | null;
    prefix?: string | null;
}

const DEFAULT_CONFIG: LoggerConfig = {
    level: LOG_LEVEL.DEBUG,
    showStackTrace: true,
    showTimestamp: true,
    format: 'text',
    storeOptions: null,
    prefix: null,
};

type NodeFs = typeof import('node:fs');
type NodePath = typeof import('node:path');
type NodeModules = { fs: NodeFs; path: NodePath };

let nodeModules: Promise<NodeModules> | undefined;

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

export class Logger {
    private logPriority: number;
    private config: LoggerConfig = DEFAULT_CONFIG;
    private storeReady?: Promise<string>;
    private storeChain: Promise<void> = Promise.resolve();
    private storeStreams: Map<string, ReturnType<NodeFs['createWriteStream']>> = new Map();

    constructor(options: LoggerConfig = DEFAULT_CONFIG) {
        this.configure(options);
        this.logPriority = LOG_PRIORITY[options.level];
    }

    configure(options: Partial<LoggerConfig>): void {
        this.config = { ...this.config, ...options };
        if (options.level) {
            this.logPriority = LOG_PRIORITY[options.level];
        }
        if (options.storeOptions !== undefined) {
            this.storeReady = undefined;
        }
    }

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

    static getTimestamp(): string {
        return new Date().toISOString().replace('T', ' - ').replace('Z', '');
    }

    static getTodaysDate(): string {
        return new Date().toISOString().split('T')[0];
    }

    private getLogFileName(level: LOG_LEVEL): string {
        const date = Logger.getTodaysDate();
        const filePrefix = this.config.storeOptions?.filePrefix ?? DEFAULT_FILE_PREFIX;
        return level === LOG_LEVEL.ERROR ? `${filePrefix}-errors-${date}.log` : `${filePrefix}-logs-${date}.log`;
    }

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

    initStore(options: StoreOptions): void {
        this.configure({ storeOptions: options });
        this.ensureStore();
    }

    setLevel(level: LOG_LEVEL): void {
        this.logPriority = LOG_PRIORITY[level];
    }

    error<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.ERROR, args);
    }

    warn<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.WARN, args);
    }

    info<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.INFO, args);
    }

    trace<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.TRACE, args);
    }

    debug<T extends unknown[]>(...args: T): void {
        this.log(LOG_LEVEL.DEBUG, args);
    }
}

export const logger = new Logger();
