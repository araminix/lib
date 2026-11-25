import type { Connection } from '../connection/index.js';
import { NewStreamType } from '../protocol/messages.js';
import { uuid } from '../utils/index.js';
import { deal, Err, Ok, Result } from '../utils/result.js';
import { Stream } from './Stream.js';
import { StreamController } from './StreamController.js';
import { InitStreamErr } from './types.js';

/**
 * Controller of a stream that the peer opened and this side accepted.
 *
 * It takes its id and arguments from the `new_stream` message and actively checks the peer: after 10 ticks without receiving
 * anything it pings every 2 ticks, and closes as `Lost` after 10 more (strong controller, `hp` 10). {@link Connection} creates it
 * when an `acquireStream()` call is matched with a queued request. Not re-exported from the package entry point.
 *
 * @typeParam TArgs - Type of the stream arguments.
 * @typeParam TDataIn - Type of the messages read from the stream.
 * @typeParam TDataOut - Type of the messages written to the stream.
 *
 * @example
 * ```ts
 * // What Connection does internally once a new_stream request is accepted (it also registers the controller by id):
 * const controller = new IncomingStreamController(connection, { type: 'new_stream', id: 's-1', name: 'chat', args: { room: 1 } });
 * controller.stream.id; // 's-1'
 * controller.stream.args; // { room: 1 }
 * ```
 */
export class IncomingStreamController<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> extends StreamController<TArgs, TDataIn, TDataOut> {
    /** The `new_stream` message this stream was opened with. */
    message: NewStreamType;
    /** Heartbeat budget: 10 ticks before pinging, then 10 more before giving up. */
    protected hp: number = 10;
    /** Strong: pings the peer before giving up. */
    protected weak: boolean = false;

    /**
     * Creates the controller for an accepted `new_stream` request.
     *
     * @param connection - The connection the stream belongs to.
     * @param message - The validated `new_stream` message (provides the id and the arguments).
     * @param incomingQueueCap - Initial incoming queue cap. Defaults to 256.
     * @param outgoingQueueCap - Initial outgoing queue cap. Defaults to 256.
     */
    constructor(connection: Connection, message: NewStreamType, incomingQueueCap: number = 256, outgoingQueueCap: number = 256) {
        super(connection, incomingQueueCap, outgoingQueueCap);
        this.message = message;
    }

    /** The `args` of the `new_stream` message, cast to `TArgs` (not validated). */
    get args(): TArgs {
        return this.message.args as TArgs;
    }

    /** The id chosen by the peer in the `new_stream` message. */
    get id(): string {
        return this.message.id;
    }
}

/**
 * Controller of a stream that this side opens.
 *
 * It generates the stream id with `uuid()`, sends `new_stream` (retrying on every tick until the frame can be sent, e.g. after a
 * reconnect) and resolves {@link OutgoingStreamController.initiate} once the peer replies with `accept_stream`. It never pings:
 * it answers the peer's pings and closes as `Lost` after 20 ticks without receiving anything (weak controller, `hp` 20), which
 * also bounds how long opening may take. Not re-exported from the package entry point; use `Socket.outgoingStream()`.
 *
 * @typeParam TArgs - Type of the stream arguments.
 * @typeParam TDataIn - Type of the messages read from the stream.
 * @typeParam TDataOut - Type of the messages written to the stream.
 *
 * @example
 * ```ts
 * // Connection.newOutgoingStream() creates the controller, registers it by id and calls initiate():
 * const opened = await connection.newOutgoingStream('svc', { a: 1 });
 * if (opened.isOk()) {
 *     await opened.unwrap().write('hello');
 * }
 * ```
 */
export class OutgoingStreamController<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> extends StreamController<TArgs, TDataIn, TDataOut> {
    /** The stream name sent in `new_stream`. */
    private readonly name: string;
    /** The arguments sent in `new_stream`; the caller's object, not copied or frozen. */
    readonly args: TArgs;
    /** Heartbeat budget: 20 ticks without receiving anything before giving up. */
    protected hp: number = 20;
    /** Weak: never pings, only answers pings. */
    protected weak: boolean = true;
    /** The stream id, a fresh UUID v4. */
    readonly id: string = uuid();
    /** Resolver of the pending {@link OutgoingStreamController.initiate} promise, until the stream is accepted or destroyed. */
    private initResolve?: (val: Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>) => void;
    /** Whether `new_stream` has been sent successfully. */
    private initSend: boolean = false;

    /**
     * Creates the controller. Nothing is sent until {@link OutgoingStreamController.initiate} is called.
     *
     * @param connection - The connection the stream belongs to. The controller must also be registered in it (see
     * `Connection.newOutgoingStream()`) to receive `accept_stream`.
     * @param name - Stream name the peer acquires with `incomingStream(name)`.
     * @param args - JSON-serialisable arguments sent with `new_stream`.
     * @param incomingQueueCap - Initial incoming queue cap. Defaults to 256.
     * @param outgoingQueueCap - Initial outgoing queue cap. Defaults to 256.
     */
    constructor(connection: Connection, name: string, args: TArgs, incomingQueueCap: number = 256, outgoingQueueCap: number = 256) {
        super(connection, incomingQueueCap, outgoingQueueCap);
        this.name = name;
        this.args = args;
    }

    /**
     * Sends `new_stream` (now, or on a later tick if the socket cannot send yet) and waits for the peer's answer.
     *
     * @returns `Ok(stream)` when the peer accepts, or `Err(InitStreamErr.Dead)` when the stream is closed first (rejected by the
     * peer, 20 ticks without an answer, or the connection was destroyed). Never rejects.
     */
    initiate(): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>> {
        const [promise, resolve] = deal<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>>();
        this.initResolve = resolve;
        this.tryInitiate();
        return promise;
    }

    /** Sends `new_stream` unless it was already sent successfully. */
    private tryInitiate(): void {
        if (!this.initSend) {
            const result = this.connection.writeNewStream(this.id, this.name, this.args);
            if (result.isOk()) {
                this.initSend = true;
            }
        }
    }

    /** Handles `accept_stream`: refreshes the heartbeat and resolves the pending {@link OutgoingStreamController.initiate} call. */
    onAcceptStream(): void {
        if (this.initResolve) {
            this.restoreHp();
            this.initResolve(new Ok(this.stream));
            this.initResolve = undefined;
        }
    }

    /** Resolves a still pending {@link OutgoingStreamController.initiate} call with {@link InitStreamErr.Dead}. */
    protected onDestroy(): void {
        if (this.initResolve) {
            this.initResolve(new Err(InitStreamErr.Dead));
            this.initResolve = undefined;
        }
    }

    /** Retries sending `new_stream` on every tick until it succeeded once. */
    protected tic(): void {
        this.tryInitiate();
    }
}
