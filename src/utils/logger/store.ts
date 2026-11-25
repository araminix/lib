import { formatTodaysDate } from './format.js';
import { DEFAULT_FILE_PREFIX, LOG_LEVEL, StoreOptions } from './types.js';

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
 * Manages daily append-only log files for a {@link Logger} in Node.js without bundling `node:fs` or `node:path` in browser builds.
 */
export class LogFileStore {
    /** Resolves with the absolute log directory once it exists; cleared when `storeOptions` change. */
    private ready?: Promise<string>;
    /** Chain that serializes file writes so that lines keep their order. */
    chain: Promise<void> = Promise.resolve();
    /** Open append streams by absolute file path. */
    streams: Map<string, ReturnType<NodeFs['createWriteStream']>> = new Map();

    /** Clears the cached directory promise so the next write re-initializes the target directory. */
    reset(): void {
        this.ready = undefined;
    }

    /** File name for today's date: the errors file for `ERROR`, the logs file for every other level. */
    private getLogFileName(level: LOG_LEVEL, filePrefix: string): string {
        const date = formatTodaysDate();
        return level === LOG_LEVEL.ERROR ? `${filePrefix}-errors-${date}.log` : `${filePrefix}-logs-${date}.log`;
    }

    /** Starts resolving and creating the log directory (once per store configuration) and returns that shared promise. */
    ensureReady(directory: string): Promise<string> {
        if (!this.ready) {
            this.ready = loadNodeModules().then(({ fs, path }) => {
                const absolutePath = path.resolve(directory);
                fs.mkdirSync(absolutePath, { recursive: true });
                return absolutePath;
            });
            // Mark as handled right away; append() reports the failure once it reaches this point in its chain.
            this.ready.catch(() => {});
        }
        return this.ready;
    }

    /** Appends a line to the right file in the background, in order; failures are reported with `console.error`. */
    append(options: StoreOptions, level: LOG_LEVEL, line: string): void {
        const fileName = this.getLogFileName(level, options.filePrefix ?? DEFAULT_FILE_PREFIX);
        const ready = this.ensureReady(options.directory);
        this.chain = this.chain
            .then(async () => {
                const [directory, { fs, path }] = await Promise.all([ready, loadNodeModules()]);
                const filePath = path.join(directory, fileName);
                let stream = this.streams.get(filePath);
                if (!stream) {
                    // The logs and errors files of the current day stay open side by side; only streams for an earlier day or
                    // another directory are released.
                    const datedSuffix = `-${formatTodaysDate()}.log`;
                    this.streams.forEach((oldStream, oldPath) => {
                        if (path.dirname(oldPath) !== directory || !oldPath.endsWith(datedSuffix)) {
                            oldStream.end();
                            this.streams.delete(oldPath);
                        }
                    });
                    stream = fs.createWriteStream(filePath, { flags: 'a' });
                    stream.on('error', (error) => console.error('Logger store write failed:', error));
                    this.streams.set(filePath, stream);
                }
                stream.write(line);
            })
            .catch((error: unknown) => {
                console.error('Logger store unavailable:', error);
            });
    }
}
