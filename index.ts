/**
 * `@araminix/lib`: typed, multiplexed WebSocket streams with a functional `Result` API, for browsers and Node.js >= 22.
 *
 * This entry point exports the {@link Client} (HTTP helpers and WebSocket endpoints), the multiplexing layer ({@link Socket},
 * {@link Connection}, {@link Stream}), the wire protocol codecs (the `Message` namespace) and the utilities ({@link Result},
 * {@link loop}, {@link Logger}, ...).
 *
 * @example
 * ```ts
 * import { Client, LoopResult, loop } from '@araminix/lib';
 *
 * const client = new Client({ host: 'https://api.example.com', wsHost: 'wss://api.example.com' });
 * const connected = await client.connect('platform');
 * if (connected.isOk()) {
 *     const socket = connected.unwrap();
 *     loop(async () => {
 *         const incoming = await socket.incomingStream('notifications');
 *         if (incoming.isErr()) return LoopResult.Stop;
 *         const message = await incoming.unwrap().read();
 *         if (message.isOk()) console.log(message.unwrap());
 *         return LoopResult.Cont;
 *     });
 * }
 * ```
 *
 * @packageDocumentation
 */
import * as messages from './src/messages.js';
import { InitStreamErr, Stream, StreamCloseCodes, StreamErr } from './src/stream.js';

// Socket, SocketErr and ConnectionStatus.
export * from './src/Socket.js';

// Connection, ConnectionInterface, MinimalWebSocket, WsCloseCodes and CloseCodes.
export * from './src/connection.js';
// Result helpers, loop/once/wrap/wrapSync/sleep/uuid/trimSlash, LoopResult, Defined, the io-ts namespace `t`, and the logger.
export * from './src/utils/index.js';
// Client, ClientOptions, ConnectFail, FileInfo, AgentClient and its error types, axiosService.
export * from './src/client.js';
// The stream API and its enums, plus the protocol codecs as the `Message` namespace (e.g. `Message.NewStream.is(value)`).
// The stream controllers in src/stream.ts are internal and deliberately not exported.
export { Stream, StreamErr, InitStreamErr, messages as Message, StreamCloseCodes };
