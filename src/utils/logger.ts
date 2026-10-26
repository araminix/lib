/**
 * Logger module for handling application logging with different levels and formats
 * @module logger
 */
import fs from 'fs';
import gach, { COLOR_NAMES } from 'gach';
import path from 'path';

export enum LOG_LEVEL {
    TRACE = 'trace',
    INFO = 'info',
    DEBUG = 'debug',
    WARN = 'warn',
    ERROR = 'error',
}

const LOG_PRIORITY: Record<LOG_LEVEL, number> = {
    [LOG_LEVEL.TRACE]: 0,
    [LOG_LEVEL.INFO]: 1,
    [LOG_LEVEL.DEBUG]: 2,
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

export interface StoreOptions {
    directory: string;
    level: LOG_LEVEL;
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

export class Logger {
    private logPriority: number;
    private config: LoggerConfig = DEFAULT_CONFIG;

    constructor(options: LoggerConfig = DEFAULT_CONFIG) {
        this.configure(options);
        this.logPriority = LOG_PRIORITY[options.level];
    }

    configure(options: Partial<LoggerConfig>): void {
        this.config = { ...this.config, ...options };
        if (options.level) {
            this.logPriority = LOG_PRIORITY[options.level];
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
                    message: args
                        .map((arg) => {
                            try {
                                return typeof arg === 'object' ? JSON.stringify(arg) : String(arg);
                            } catch {
                                return '[Unserializable Object]';
                            }
                        })
                        .join(' '),
                };
                consoleFn(JSON.stringify(logObject));
            } else {
                const coloredLevel = (gach as any).default(`[${level.toUpperCase()}]`).color(LOG_COLORS[level]).text;
                const coloredTime = this.config.showTimestamp ? (gach as any).default(`[${timestamp}]`).color('blue').text + ' ' : '';
                consoleFn(`${coloredTime}${coloredLevel}${prefix}`, ...args);
            }
        }

        if (this.config.storeOptions && LOG_PRIORITY[level] >= LOG_PRIORITY[this.config.storeOptions.level]) {
            const formattedArgs = args.map((arg) => (typeof arg === 'object' ? JSON.stringify(arg) : String(arg))).join(' ');
            const logMessage = `[${timestamp}] [${level.toUpperCase()}] ${formattedArgs}`;
            const filePath = this.getLogFilePath(level);
            fs.appendFileSync(filePath, logMessage + '\n');
        }
    }

    static getTimestamp(): string {
        return new Date().toISOString().replace('T', ' - ').replace('Z', '');
    }

    static getTodaysDate(): string {
        return new Date().toISOString().split('T')[0];
    }

    private getLogFilePath(level: LOG_LEVEL): string {
        if (!this.config.storeOptions) {
            throw new Error('Log directory is not initialized. Call initStore first.');
        }
        const date = Logger.getTodaysDate();
        const fileName = level === LOG_LEVEL.ERROR ? `folkrum-errors-${date}.log` : `folkrum-logs-${date}.log`;
        return path.join(this.config.storeOptions.directory, fileName);
    }

    initStore(options: StoreOptions): void {
        const { directory, level } = options;
        const absolutePath = path.resolve(directory);
        if (!fs.existsSync(absolutePath)) {
            fs.mkdirSync(absolutePath, { recursive: true });
        }
        this.config.storeOptions = { directory: absolutePath, level };
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
