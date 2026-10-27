import { AcceptEndpoint, AcceptStream, CloseStream, DataStream, NewStream, PingStream, PongStream } from './messages.js';
import { SocketErr } from './Socket.js';
import { IncomingStreamController, InitStreamErr, OutgoingStreamController, Stream, StreamCloseCodes as StreamCloseCode, StreamController } from './stream.js';
import { logger } from './utils/logger.js';
import { deal, err, Err, ok, Ok, R, Result } from './utils/result.js';

export interface MinimalWebSocket {
    send(data: string): void;
    close(code?: number, reason?: string): void;
    onopen: ((event: any) => void) | null;
    onerror: ((event: any) => void) | null;
    onclose: ((event: { code: number; reason?: string }) => void) | null;
    onmessage: ((event: { data: any }) => void) | null;
    readyState?: number;
}

const DEFAULT_HP = 300; // seconds (60 * 5) 5 minutes

class StreamRequest {
    message: { id: string; name: string; args: unknown };
    onCancel: () => void;
    hp: number = 10;

    constructor(message: { id: string; name: string; args: unknown }, onCancel: () => void) {
        this.message = message;
        this.onCancel = onCancel;
    }

    damage(): void {
        this.hp--;
        if (this.hp === 0) {
            this.onCancel();
        }
    }

    get id(): string {
        return this.message.id;
    }
}

export class Connection {
    private readonly inter: ConnectionInterface;
    arg: string;
    ws: MinimalWebSocket | undefined = undefined;
    private hp: number;
    private streamQueue: Map<string, StreamRequest[]> = new Map();
    private promises: Map<string, (result: Result<Stream<any, any, any>, SocketErr>) => void> = new Map();
    private streams: Map<string, StreamController<any, any, any>> = new Map();
    private accepted: boolean = false;
    readonly closePromise: Promise<CloseCodes>;
    private resolveClose: (code: CloseCodes) => void;
    dead: boolean = false;
    private timer: NodeJS.Timeout = setInterval(() => {
        this.tic();
    }, 1000);

    constructor(inter: ConnectionInterface, arg: string) {
        this.inter = inter;
        this.arg = arg;
        this.hp = inter.hp ?? DEFAULT_HP;
        [this.closePromise, this.resolveClose] = deal<CloseCodes>();
        if (this.timer && typeof this.timer.unref === 'function') {
            this.timer.unref();
        }
    }

    removeStream(id: string): void {
        this.streams.delete(id);
    }

    async newOutgoingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string, args: TArgs): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>> {
        if (this.dead) {
            return new Err(InitStreamErr.Dead);
        }
        const stream = new OutgoingStreamController<TArgs, TDataIn, TDataOut>(this, name, args);
        this.streams.set(stream.id, stream);
        return await stream.initiate();
    }

    accept(): R {
        if (this.dead) {
            return err();
        }
        const result = this.write({
            type: 'accept_endpoint',
        });
        if (result.isErr()) {
            this.destroy(CloseCodes.Unexpected);
            return result;
        }
        this.accepted = true;
        return result;
    }

    deny(): void {
        this.ws?.close(WsCloseCodes.Denied);
        this.destroy(CloseCodes.Unauthorized);
    }

    writeAcceptStream(streamId: string): R {
        const message = {
            type: 'accept_stream',
            id: streamId,
        };
        return this.write(message);
    }

    writeNewStream<TArgs = unknown>(streamId: string, name: string, args: TArgs): R {
        const message = {
            type: 'new_stream',
            id: streamId,
            name,
            args,
        };
        return this.write(message);
    }

    writeData<T = unknown>(streamId: string, data: T): R {
        const message = {
            type: 'data_stream',
            id: streamId,
            data,
        };
        return this.write(message);
    }

    writeClose(streamId: string, code: StreamCloseCode): R {
        const message = {
            type: 'close_stream',
            id: streamId,
            code,
        };
        return this.write(message);
    }

    writePing(streamId: string): R {
        const message = {
            type: 'ping_stream',
            id: streamId,
        };
        return this.write(message);
    }

    writePong(streamId: string): R {
        const message = {
            type: 'pong_stream',
            id: streamId,
        };
        return this.write(message);
    }

    acquireStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>> {
        const [promise, resolve] = deal<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>>();
        if (this.dead) {
            resolve(new Err(SocketErr.Closed));
            return promise;
        }
        const existingPromise = this.promises.get(name);
        if (existingPromise) {
            existingPromise(new Err(SocketErr.Concurrent));
        }
        this.promises.set(name, resolve as (result: Result<Stream<any, any, any>, SocketErr>) => void);
        this.roll();
        return promise;
    }

    private roll(): void {
        for (const [name, promise] of this.promises) {
            const streams = this.streamQueue.get(name);
            if (!streams) {
                continue;
            }
            const streamRequest = streams.shift();
            if (!streamRequest) {
                continue;
            }
            const result = this.writeAcceptStream(streamRequest.id);
            if (result.isErr()) {
                streams.unshift(streamRequest);
                return;
            }
            const stream = this.startStream(streamRequest.message as any);
            this.promises.delete(name);
            promise(new Ok(stream));
        }
    }

    private startStream(message: any): Stream<any, any, any> {
        const streamController = new IncomingStreamController(this, message);
        this.streams.set(streamController.id, streamController);
        return streamController.stream;
    }

    connect(ws: MinimalWebSocket, reset: boolean = true): void {
        if (this.dead) {
            ws.close(WsCloseCodes.Normal);
            return;
        }
        if (reset) {
            this.hp = this.inter.hp ?? DEFAULT_HP;
        }
        const oldWs = this.ws;
        Object.defineProperty(this, 'ws', {
            value: ws,
            writable: true,
            configurable: true,
            enumerable: true,
        });
        if (oldWs) {
            oldWs.close(WsCloseCodes.ConcurrentConnection);
        }
        ws.onopen = () => {
            if (this.ws === ws) {
                this.hp = this.inter.hp ?? DEFAULT_HP;
                this.roll();
                this.inter.onOpen?.();
            }
        };
        ws.onerror = (event: any) => {
            logger.error(`WS Connection - onError:`, event);
        };
        ws.onclose = (event: { code: number }) => {
            if (this.ws === ws) {
                Object.defineProperty(this, 'ws', {
                    value: undefined,
                    writable: true,
                    configurable: true,
                    enumerable: true,
                });
                this.inter.onDisconnect(event.code);
                logger.warn(`WS Connection - onClose, code: ${event.code}`);
            }
        };
        ws.onmessage = (message: { data: any }) => {
            if (this.ws === ws) {
                try {
                    const raw = typeof message.data === 'string' ? message.data : message.data.toString();
                    const data = JSON.parse(raw);
                    logger.trace(`WS Connection - onMessage: ${JSON.stringify(data)}`);
                    this.onIncoming(data);
                } catch (error) {
                    logger.error(`WS Connection - onMessage JSON Parse Error:`, error);
                }
            }
        };
        this.roll();
    }

    private onIncoming(data: unknown): void {
        switch (true) {
            case NewStream.is(data): {
                this.onNewStream(data);
                break;
            }
            case DataStream.is(data): {
                this.onData(data);
                break;
            }
            case CloseStream.is(data): {
                this.onCloseStream(data);
                break;
            }
            case PingStream.is(data): {
                this.onPingStream(data);
                break;
            }
            case PongStream.is(data): {
                this.onPongStream(data);
                break;
            }
            case AcceptStream.is(data): {
                this.onAcceptStream(data);
                break;
            }
            case AcceptEndpoint.is(data): {
                if (this.accepted) {
                    break;
                }
                this.accepted = true;
                this.inter.onAccept();
                break;
            }
        }
    }

    private tic(): void {
        if (!this.ws) {
            this.hp--;
        }
        if (this.hp === 0) {
            this.destroy(CloseCodes.Died);
            return;
        }
        this.streamQueue.forEach((streams) => {
            streams.forEach((stream) => stream.damage());
        });
        this.streams.forEach((stream) => {
            stream._tic();
        });
    }

    private onAcceptStream(data: { id: string }): void {
        const stream = this.streams.get(data.id);
        if (!stream || !(stream instanceof OutgoingStreamController)) {
            this.writeClose(data.id, StreamCloseCode.NotFound);
            return;
        }
        stream.onAcceptStream();
    }

    private onPongStream(data: { id: string }): void {
        const stream = this.streams.get(data.id);
        if (!stream) {
            this.writeClose(data.id, StreamCloseCode.NotFound);
            return;
        }
        stream.onPong();
    }

    private onPingStream(data: { id: string }): void {
        const stream = this.streams.get(data.id);
        if (!stream) {
            this.writeClose(data.id, StreamCloseCode.NotFound);
            return;
        }
        stream.onPing();
    }

    private onData(data: { id: string; data: unknown }): void {
        const stream = this.streams.get(data.id);
        if (!stream) {
            this.writeClose(data.id, StreamCloseCode.NotFound);
            return;
        }
        stream.onData(data.data);
    }

    private onNewStream(data: { id: string; name: string; args: unknown }): void {
        if (this.streams.has(data.id)) {
            this.writeClose(data.id, StreamCloseCode.Duplicate);
            return;
        }
        const streamRequest = new StreamRequest(data, () => {
            const requests = this.streamQueue.get(data.name);
            if (!requests) {
                return;
            }
            const newRequests = requests.filter((request) => request.id !== data.id);
            if (newRequests.length === requests.length) {
                return;
            }
            this.writeClose(data.id, StreamCloseCode.Lost);
            if (newRequests.length === 0) {
                this.streamQueue.delete(data.name);
                return;
            }
            this.streamQueue.set(data.name, newRequests);
        });
        if (!this.streamQueue.has(data.name)) {
            this.streamQueue.set(data.name, []);
        }
        this.streamQueue.get(data.name)!.push(streamRequest);
        this.roll();
    }

    private write(message: unknown): R {
        let payload: string;
        try {
            payload = JSON.stringify(message);
        } catch {
            return err();
        }
        const ws = this.ws;
        if (!ws || ws.readyState !== 1) {
            return err();
        }
        try {
            ws.send(payload);
        } catch {
            try {
                ws.close();
            } catch {
                // ignore
            }
            return err();
        }
        return ok();
    }

    private onCloseStream(data: { id: string; code: string }): void {
        const stream = this.streams.get(data.id);
        if (!stream) {
            return;
        }
        stream.onClose(data.code as StreamCloseCode);
    }

    destroy(closeCode: CloseCodes): void {
        if (this.dead) return;
        this.dead = true;
        this.resolveClose(closeCode);
        clearInterval(this.timer);
        this.streams.forEach((stream) => {
            stream.close(StreamCloseCode.Lost);
        });
        this.streamQueue.forEach((requests) => {
            requests.forEach((request) => {
                this.writeClose(request.id, StreamCloseCode.Lost);
            });
        });
        this.streamQueue.clear();
        if (this.ws) {
            this.ws.close(WsCloseCodes.Normal);
        }
        this.promises.forEach((promise) => {
            promise(new Err(SocketErr.Closed));
        });
        this.promises.clear();
        this.inter.onDestroy();
    }
}

export interface ConnectionInterface {
    hp?: number;
    onDestroy: () => void;
    onDisconnect: (code: number) => void;
    onAccept: () => void;
    onOpen?: () => void;
}

export enum WsCloseCodes {
    InternalError = 1011,
    NotFound = 4404,
    ConcurrentConnection = 4409,
    Unauthorized = 4401,
    Denied = 4403,
    Normal = 4000,
}

export enum CloseCodes {
    Died = 'Died',
    Unexpected = 'Unexpected',
    Intentional = 'Intentional',
    Concurrent = 'Concurrent',
    Unauthorized = 'Unauthorized',
}
