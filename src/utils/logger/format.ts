import gach from 'gach';

/** Wraps text in ANSI color codes via gach. */
// Node's ESM loader exposes gach's CommonJS exports object (function on `.default`); bundlers often hand over the function itself.
export const paint: (text: string) => { color: (name: string) => { text: string } } = (gach as any).default ?? gach;

/**
 * Returns the current UTC time in the format used by log output: `YYYY-MM-DD - HH:mm:ss.sss`.
 */
export function formatTimestamp(): string {
    return new Date().toISOString().replace('T', ' - ').replace('Z', '');
}

/**
 * Returns the current UTC date as `YYYY-MM-DD`, as used in log file names.
 */
export function formatTodaysDate(): string {
    return new Date().toISOString().split('T')[0];
}

/**
 * Serializes one log argument for JSON output and log files: errors as stack (or `Name: message`), objects as JSON that
 * survives circular references, BigInt and nested errors, everything else with `String()`.
 */
export function stringifyArg(arg: unknown, showStackTrace: boolean): string {
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
