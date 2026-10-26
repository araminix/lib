import type { Connection } from './connection.js';
import { NewStreamType } from './messages.js';
import { uuid } from './utils/index.js';
import { deal, Err, Ok, Result } from './utils/result.js';

export enum StreamErr {
    Dead = 'Dead',
    Concurrent = 'Concurrent',
    Timeout = 'Timeout',
}

export enum InitStreamErr {
    Dead = 'Dead',
    Lost = 'Lost',
}

export enum StreamCloseCodes {
    Normal = 'Normal',
    Lost = 'Lost',
    NotFound = 'NotFound',
    Duplicate = 'Duplicate',
    Error = 'Error',
}

export class Stream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> {
    private controller: StreamController<TArgs, TDataIn, TDataOut>;

    constructor(controller: StreamController<TArgs, TDataIn, TDataOut>) {
        this.controller = controller;
    }

    get active(): boolean {
        return !this.controller.dead;
    }

    waitClose(): Promise<StreamCloseCodes> {
        return this.controller.closePromise;
    }

    get closeCode(): StreamCloseCodes | undefined {
        return this.controller.closedCode;
    }

    get id(): string {
        return this.controller.id;
    }

    get args(): Readonly<TArgs> {
        return Object.freeze(this.controller.args);
    }

    read<T = TDataIn>(hp: number = 30): Promise<Result<T, StreamErr>> {
        return this.controller.read<T>(hp);
    }

    async write<T = TDataOut>(data: T): Promise<Result<void, StreamErr>> {
        return this.controller.write(data);
    }

    closeOk(): void {
        this.controller.close(StreamCloseCodes.Normal);
    }

    closeErr(): void {
        this.controller.close(StreamCloseCodes.Error);
    }
}

export abstract class StreamController<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> {
    protected connection: Connection;
    stream: Stream<TArgs, TDataIn, TDataOut> = new Stream(this);
    protected initiated: boolean = false;
    private _hp?: number;
    private _weak: boolean = false;
    protected abstract hp: number;
    protected abstract weak: boolean;
    private incomingData: unknown = undefined;
    private outgoingData: unknown = undefined;
    dead: boolean = false;
    closedCode: StreamCloseCodes | undefined = undefined;
    private readResolve?: (val: Result<any, StreamErr>) => void;
    private writeResolve?: (val: Result<void, StreamErr>) => void;
    private readHp?: number;
    readonly closePromise: Promise<StreamCloseCodes>;
    private closeResolve: (val: StreamCloseCodes) => void;

    constructor(connection: Connection) {
        this.connection = connection;
        [this.closePromise, this.closeResolve] = deal<StreamCloseCodes>();
    }

    abstract get id(): string;
    abstract get args(): TArgs;

    protected restoreHp(): void {
        this._weak = false;
        this._hp = this.hp;
    }

    close(code: StreamCloseCodes): void {
        this.closedCode = code;
        this.connection.writeClose(this.id, code);
        this.destroy();
    }

    write<T = TDataOut>(data: T): Promise<Result<void, StreamErr>> {
        const [promise, resolve] = deal<Result<void, StreamErr>>();
        if (this.dead) {
            resolve(new Err(StreamErr.Dead));
            return promise;
        }
        if (this.writeResolve) {
            this.writeResolve(new Err(StreamErr.Concurrent));
            this.outgoingData = undefined;
        }
        const result = this.connection.writeData(this.id, data);
        if (result.isOk()) {
            resolve(new Ok(undefined));
            return promise;
        }
        this.writeResolve = resolve;
        this.outgoingData = data;
        return promise;
    }

    read<T = TDataIn>(hp: number): Promise<Result<T, StreamErr>> {
        this.readHp = hp;
        const [promise, resolve] = deal<Result<T, StreamErr>>();
        if (this.dead) {
            resolve(new Err(StreamErr.Dead));
            return promise;
        }
        if (this.incomingData !== undefined) {
            resolve(new Ok(this.incomingData as T));
            this.incomingData = undefined;
            return promise;
        }
        if (this.readResolve) {
            this.readResolve(new Err(StreamErr.Concurrent));
        }
        this.readResolve = resolve;
        return promise;
    }

    onClose(code: StreamCloseCodes): void {
        this.closedCode = code;
        this.destroy();
    }

    onData<T = TDataIn>(data: T): void {
        this.restoreHp();
        if (this.readResolve) {
            this.readResolve(new Ok(data));
            this.readResolve = undefined;
            return;
        }
        this.incomingData = data;
    }

    onPong(): void {
        this.restoreHp();
    }

    onPing(): void {
        this.restoreHp();
        this.connection.writePong(this.id);
    }

    _tic(): void {
        if (this._hp === undefined) {
            this._hp = this.hp;
        }
        this._hp--;
        if (this._hp === 0 && (this.weak || this._weak)) {
            this.closedCode = StreamCloseCodes.Lost;
            this.destroy();
            return;
        }
        if (this._hp === 0 && !this._weak) {
            this._weak = true;
            this._hp = this.hp;
        }
        if (this._weak && !this.weak && this._hp % 2 === 0) {
            this.connection.writePing(this.id);
        }
        if (this.readResolve && this.readHp !== undefined) {
            this.readHp--;
            if (this.readHp === 0) {
                this.readResolve(new Err(StreamErr.Timeout));
                this.readResolve = undefined;
            }
        }
        if (!this._weak && this.writeResolve) {
            const result = this.connection.writeData(this.id, this.outgoingData);
            if (result.isOk()) {
                this.writeResolve(new Ok(undefined));
                this.outgoingData = undefined;
            }
        }
        if (!this.weak) {
            this.tic();
        }
    }

    protected tic(): void {}

    private destroy(): void {
        this.dead = true;
        if (this.readResolve) {
            this.readResolve(new Err(StreamErr.Dead));
            this.readResolve = undefined;
        }
        if (this.writeResolve) {
            this.writeResolve(new Err(StreamErr.Dead));
            this.writeResolve = undefined;
            this.outgoingData = undefined;
        }
        this.connection.removeStream(this.id);
        this.onDestroy();
        if (this.closedCode) {
            this.closeResolve(this.closedCode);
        }
    }

    protected onDestroy(): void {}
}

export class IncomingStreamController<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> extends StreamController<TArgs, TDataIn, TDataOut> {
    message: NewStreamType;
    protected hp: number = 10;
    protected weak: boolean = false;

    constructor(connection: Connection, message: NewStreamType) {
        super(connection);
        this.message = message;
    }

    get args(): TArgs {
        return this.message.args as TArgs;
    }

    get id(): string {
        return this.message.id;
    }
}

export class OutgoingStreamController<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> extends StreamController<TArgs, TDataIn, TDataOut> {
    private readonly name: string;
    readonly args: TArgs;
    protected hp: number = 20;
    protected weak: boolean = true;
    readonly id: string = uuid();
    private initResolve?: (val: Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>) => void;
    private initSend: boolean = false;

    constructor(connection: Connection, name: string, args: TArgs) {
        super(connection);
        this.name = name;
        this.args = args;
    }

    initiate(): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>> {
        const [promise, resolve] = deal<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>>();
        this.initResolve = resolve;
        this.tryInitiate();
        return promise;
    }

    private tryInitiate(): void {
        if (!this.initSend) {
            const result = this.connection.writeNewStream(this.id, this.name, this.args);
            if (result.isOk()) {
                this.initSend = true;
            }
        }
    }

    onAcceptStream(): void {
        if (this.initResolve) {
            this.restoreHp();
            this.initResolve(new Ok(this.stream));
            this.initResolve = undefined;
        }
    }

    protected onDestroy(): void {
        if (this.initResolve) {
            this.initResolve(new Err(InitStreamErr.Dead));
            this.initResolve = undefined;
        }
    }

    protected tic(): void {
        this.tryInitiate();
    }
}
