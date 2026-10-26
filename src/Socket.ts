import { CloseCodes, Connection } from './connection.js';
import { InitStreamErr, Stream } from './stream.js';
import { Result } from './utils/result.js';

export enum SocketErr {
    Closed = 0,
    Concurrent = 1,
}

export enum ConnectionStatus {
    Connecting = 'connecting',
    Open = 'open',
    Closing = 'closing',
    Closed = 'closed',
    Unknown = 'unknown',
}

export class Socket {
    connection: Connection;

    constructor(connection: Connection) {
        this.connection = connection;
    }

    async incomingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>> {
        return await this.connection.acquireStream<TArgs, TDataIn, TDataOut>(name);
    }

    get arg(): string {
        return this.connection.arg;
    }

    async waitClose(): Promise<CloseCodes> {
        return this.connection.closePromise;
    }

    isDead(): boolean {
        return this.connection?.dead === true;
    }

    destroy(): void {
        this.connection.destroy(CloseCodes.Intentional);
    }

    outgoingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string, args?: TArgs): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>> {
        return this.connection.newOutgoingStream<TArgs, TDataIn, TDataOut>(name, args as TArgs);
    }

    getConnectionStatus(): ConnectionStatus {
        try {
            const ws = this.connection?.ws;
            if (!ws) return ConnectionStatus.Unknown;
            switch (ws.readyState) {
                case 0:
                    return ConnectionStatus.Connecting;
                case 1:
                    return ConnectionStatus.Open;
                case 2:
                    return ConnectionStatus.Closing;
                case 3:
                    return ConnectionStatus.Closed;
                default:
                    return ConnectionStatus.Unknown;
            }
        } catch {
            return ConnectionStatus.Unknown;
        }
    }
}
