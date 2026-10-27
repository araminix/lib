# @araminix/lib

> **Universal, type-safe multiplexed WebSocket streaming and functional communication protocol.**

[![npm version](https://img.shields.io/badge/npm-3.0.2-blue.svg)](https://www.npmjs.com/package/@araminix/lib)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-7-blue.svg)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-%3E%3D22-green.svg)](https://nodejs.org/)

`@araminix/lib` is the communication layer of the Araminix stack. It runs any number of independent, typed, bidirectional **streams over a single WebSocket**, keeps them alive across network drops, and reports every failure as a [`Result`](#result) value instead of an exception. The same code runs in browsers and in Node.js 22 or later on the native `WebSocket`, and it is the client-side counterpart of [`@araminix/platform`](#related-packages), which uses the same primitives on the server.

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
    - [Connections and sockets](#connections-and-sockets)
    - [Multiplexed streams](#multiplexed-streams)
    - [Protocol messages](#protocol-messages)
    - [Heartbeats and hp](#heartbeats-and-hp)
    - [Reconnects and resumption](#reconnects-and-resumption)
    - [The Result monad](#the-result-monad)
- [Guides](#guides)
    - [Browser client with cookie authentication](#browser-client-with-cookie-authentication)
    - [Node.js agent with a token](#nodejs-agent-with-a-token)
    - [Serving incoming streams with loop](#serving-incoming-streams-with-loop)
    - [Request/response with AgentClient](#requestresponse-with-agentclient)
    - [Stream backpressure and queue caps](#stream-backpressure-and-queue-caps)
    - [Error handling with Result and UnwrapError](#error-handling-with-result-and-unwraperror)
    - [Logging and file storage](#logging-and-file-storage)
- [API reference](#api-reference)
    - [Client and agent](#client-and-agent): [`Client`](#client), [`ClientOptions`](#clientoptions), [`ConnectFail`](#connectfail), [`FileInfo`](#fileinfo), [`AgentClient`](#agentclient), [`AgentClientError`](#agentclienterror), [`AgentClientErrorType`](#agentclienterrortype), [`CommunicationClientAgentError`](#communicationclientagenterror), [`ExecutionClientAgentError`](#executionclientagenterror), [`axiosService`](#axiosservice)
    - [Sockets](#sockets): [`Socket`](#socket), [`SocketErr`](#socketerr), [`ConnectionStatus`](#connectionstatus)
    - [Connections](#connections): [`Connection`](#connection), [`ConnectionInterface`](#connectioninterface), [`MinimalWebSocket`](#minimalwebsocket), [`WsCloseCodes`](#wsclosecodes), [`CloseCodes`](#closecodes)
    - [Streams](#streams): [`Stream`](#stream), [`StreamErr`](#streamerr), [`InitStreamErr`](#initstreamerr), [`StreamCloseCodes`](#streamclosecodes)
    - [Protocol codecs](#protocol-codecs): [`Message`](#message)
    - [Results](#results): [`Result`](#result), [`Ok`](#ok), [`Err`](#err), [`UnwrapError`](#unwraperror), [`ok()`](#ok-helper), [`err()`](#err-helper), [`R`](#r), [`deal()`](#deal), [`Deal`](#deal-type)
    - [Utilities](#utilities): [`loop()`](#loop), [`LoopResult`](#loopresult), [`once()`](#once), [`wrap()`](#wrap), [`wrapSync()`](#wrapsync), [`sleep()`](#sleep), [`uuid()`](#uuid), [`trimSlash()`](#trimslash), [`Defined`](#defined), [`t`](#t-io-ts-namespace)
    - [Logging](#logging): [`Logger`](#logger), [`logger`](#logger-instance), [`LOG_LEVEL`](#log_level), [`LoggerConfig`](#loggerconfig), [`StoreOptions`](#storeoptions)
- [Configuration reference](#configuration-reference)
    - [Client options](#client-options)
    - [Connection options](#connection-options)
    - [Logger options](#logger-options)
    - [Log storage options](#log-storage-options)
- [Close codes and error enums](#close-codes-and-error-enums)
- [Migration notes](#migration-notes)
- [Testing](#testing)
- [Related packages](#related-packages)
- [License](#license)

## Features

- **Multiplexed streams.** Open as many streams as you need over one WebSocket. Each [`Stream`](#stream) has a name, typed arguments, typed messages in both directions, its own lifecycle and bounded FIFO queues.
- **Resilient connections.** The [`Client`](#client) reconnects with exponential backoff and jitter, and the [`Connection`](#connection) resumes on the new WebSocket, so a [`Socket`](#socket) survives network drops. Per-stream heartbeats detect dead peers.
- **No exceptions in the API.** Every fallible call resolves to a [`Result`](#result) ([`Ok`](#ok) or [`Err`](#err)) with a typed error such as [`ConnectFail`](#connectfail) or [`StreamErr`](#streamerr). Helpers like [`wrap()`](#wrap) and [`deal()`](#deal) bring the same style to your own code.
- **Isomorphic.** Browsers and Node.js 22+ both use the native `WebSocket`; there is no `ws` or `websocket` dependency.
- **Built-in authentication helpers.** Cookie sessions in browsers, an `x-token` header in Node.js, plus login, Google login, two-factor authentication, file uploads and presigned attachment URLs.
- **Agent request/response.** [`AgentClient`](#agentclient) sends requests with [`post()`](#agentclientpost) and serves server-initiated tasks with [`onTask()`](#agentclientontask).
- **Runtime-validated protocol.** Every frame is checked with an io-ts codec (see [`Message`](#message)); malformed frames are dropped.
- **Structured logging.** [`Logger`](#logger) with five levels, coloured text or JSON output and optional daily log files in Node.js.
- **ESM with types.** Ships compiled ES2022 modules, `.d.ts` declarations and declaration maps.

## Requirements

| Environment   | Requirement                                                                                                                                                                                                                                                                                |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Node.js       | **22 or later** (`engines.node: ">=22"`). Node.js 22 provides the global `WebSocket`, `crypto` and `FormData` the library uses.                                                                                                                                                            |
| Browsers      | Any current browser with `WebSocket`, `FormData`, `crypto.getRandomValues` and ES2022 support. `crypto.randomUUID` is optional: [`uuid()`](#uuid) falls back to `getRandomValues` on non-HTTPS pages.                                                                                      |
| WebSocket     | The native global `WebSocket` only; no extra WebSocket package is needed or used. In Node.js the token header is passed through the built-in `new WebSocket(url, { headers })`. Without a global `WebSocket`, [`Client.connect()`](#clientconnect) resolves `Err(ConnectFail.Unexpected)`. |
| Module system | ESM only (`"type": "module"`, `import` export condition).                                                                                                                                                                                                                                  |
| Bundlers      | Nothing special. The logger imports `node:fs` and `node:path` lazily, only when file storage is used, and hides those imports from webpack and Vite, so browser bundles need no polyfills.                                                                                                 |

## Installation

```bash
# npm
npm install @araminix/lib

# pnpm
pnpm add @araminix/lib

# yarn
yarn add @araminix/lib
```

## Quick start

```ts
import { Client, loop, LoopResult } from '@araminix/lib';

const client = new Client({
    host: 'https://api.example.com', // base URL for HTTP calls
    wsHost: 'wss://api.example.com', // base URL for WebSocket endpoints
    connectTimeout: 10_000, // give up if the endpoint is not accepted within 10 s
});

// Opens wss://api.example.com/ws/chat/room-42 and waits until the server accepts the endpoint.
const connected = await client.connect('chat', 'room-42');
if (connected.isErr()) {
    throw new Error(`cannot connect: ${connected.e}`);
}
const socket = connected.unwrap();

// 1. Open a stream towards the server, send a question and wait up to 10 s for the answer.
const opened = await socket.outgoingStream<{ topic: string }, string, string>('ask', { topic: 'weather' });
if (opened.isOk()) {
    const stream = opened.unwrap();
    await stream.write('Will it rain tomorrow?');
    const answer = await stream.read(10);
    console.log(answer.isOk() ? answer.unwrap() : `no answer: ${answer.e}`);
    stream.closeOk();
}

// 2. Serve the streams that the server opens towards this client.
loop(async () => {
    const incoming = await socket.incomingStream<void, string>('notifications');
    if (incoming.isErr()) return LoopResult.Stop; // the socket was destroyed
    const stream = incoming.unwrap();
    const message = await stream.read();
    if (message.isOk()) console.log('notification:', message.unwrap());
    stream.closeOk();
    return LoopResult.Cont;
});
```

## Core concepts

```
 Client (browser or Node.js)                                            Server (e.g. @araminix/platform)

 +-----------------------------+   ws(s)://host/mount/ws/chat/room-42   +-----------------------------+
 | Socket                      |                                        | Socket                      |
 |  Connection                 | <==== one WebSocket, JSON frames ====> |  Connection                 |
 |   1 s tick: hp, heartbeats  |                                        |   1 s tick: hp, heartbeats  |
 |   +- Stream "ask"           | <---- new_stream / accept_stream ----> |   +- Stream "ask"           |
 |   +- Stream "feed"          | <---- data_stream / close_stream ----> |   +- Stream "feed"          |
 |                             | <---- ping_stream / pong_stream -----> |                             |
 +-----------------------------+                                        +-----------------------------+
```

### Connections and sockets

A [`Connection`](#connection) is one logical session with a WebSocket endpoint, identified by the endpoint name and an optional argument (`/ws/<name>/<arg>`). It owns all streams of that session and **outlives individual WebSockets**: when the socket drops, the connection keeps its streams and pending work until a new WebSocket is attached.

A [`Socket`](#socket) is the application-facing wrapper around a connection. [`Client.connect()`](#clientconnect) creates the connection, opens the WebSocket and resolves with a `Socket` once the server has sent `accept_endpoint`. Servers create their own connections and call [`accept()`](#connection) or [`deny()`](#connection) on them. A socket ends for good when its connection is destroyed; [`Socket.waitClose()`](#socket) tells you why with a [`CloseCodes`](#closecodes) value.

### Multiplexed streams

A [`Stream`](#stream) is a named, bidirectional channel inside a connection:

- **Outgoing streams** are opened with [`Socket.outgoingStream(name, args)`](#socketoutgoingstream). The library generates a UUID, sends `new_stream` and resolves when the peer answers `accept_stream`.
- **Incoming streams** are accepted with [`Socket.incomingStream(name)`](#socketincomingstream). Requests from the peer are queued per name until you ask for them. Only one call per name can wait at a time (a newer call wins and the older one gets [`SocketErr.Concurrent`](#socketerr)), and a queued request that nobody acquires within `streamRequestHp` ticks (10 by default) is dropped with `Lost`.
- Each stream carries `args` (sent once, with `new_stream`) and any number of JSON messages in both directions, read with [`read()`](#streamread) and written with [`write()`](#streamwrite).
- Each stream has an **incoming queue** for messages nobody is reading yet and an **outgoing queue** for messages that cannot be sent yet. Both are FIFO and capped at 256 messages by default; overflowing either closes the stream with [`StreamCloseCodes.Error`](#streamclosecodes). See [Stream backpressure and queue caps](#stream-backpressure-and-queue-caps).
- Closing is explicit: [`closeOk()`](#stream) or [`closeErr()`](#stream) send `close_stream` to the peer. Messages that already arrived stay readable after the stream closed.

### Protocol messages

All frames are JSON text frames, validated on arrival with the io-ts codecs exported as [`Message`](#message). Frames that are not valid JSON are logged and dropped; frames that match no codec are ignored.

| Frame             | Fields               | Sent by                     | Meaning                                                                                                        |
| ----------------- | -------------------- | --------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `accept_endpoint` | none                 | server                      | The endpoint was accepted; the client's `connect()` resolves. Repeated after reconnects and then ignored.      |
| `new_stream`      | `id`, `name`, `args` | either side                 | Opens stream `id` called `name`.                                                                               |
| `accept_stream`   | `id`                 | receiver of `new_stream`    | The stream was acquired; the opener's `outgoingStream()` resolves.                                             |
| `data_stream`     | `id`, `data`         | either side                 | One application message.                                                                                       |
| `close_stream`    | `id`, `code`         | either side                 | Closes the stream with a [`StreamCloseCodes`](#streamclosecodes) value. Also the answer to unknown stream ids. |
| `ping_stream`     | `id`                 | side that accepted a stream | Heartbeat probe after 10 idle ticks.                                                                           |
| `pong_stream`     | `id`                 | side that opened a stream   | Heartbeat answer.                                                                                              |

### Heartbeats and hp

Every connection runs a single **1-second timer**. All timeouts in the protocol are counted in its ticks and are called **hp** ("health points") throughout the API: an hp value of 30 means "about 30 seconds". (Only the client's `connectTimeout` and reconnect backoff are in milliseconds.) Because ticks come from a shared interval, a countdown of `hp` ends between `hp - 1` and `hp` seconds after it starts. In Node.js the timer is `unref()`ed and never keeps the process alive.

| Countdown                                                                                              | Default | What it counts                                                             | When it reaches 0                                                                                  |
| ------------------------------------------------------------------------------------------------------ | ------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Connection hp ([`ClientOptions.hp`](#client-options), [`ConnectionInterface.hp`](#connection-options)) | 300     | Ticks without an attached WebSocket. Refilled whenever a socket opens.     | The connection is destroyed with [`CloseCodes.Died`](#closecodes).                                 |
| Stream request ([`ConnectionInterface.streamRequestHp`](#connection-options))                          | 10      | Ticks a received `new_stream` waits for `incomingStream()`.                | The request is dropped and the peer gets `close_stream` `Lost`.                                    |
| Heartbeat of a stream you **accepted**                                                                 | 10 + 10 | Ticks without receiving any frame on the stream.                           | After 10: the stream pings every 2 ticks and queues writes. After 10 more: it is closed as `Lost`. |
| Heartbeat of a stream you **opened**                                                                   | 20      | Ticks without receiving any frame, including the wait for `accept_stream`. | The stream is closed as `Lost`; a pending `outgoingStream()` resolves `InitStreamErr.Dead`.        |
| [`Stream.read(hp)`](#streamread)                                                                       | 30      | Ticks waiting for the next message.                                        | The read resolves `Err(StreamErr.Timeout)`; the stream stays open.                                 |
| [`AgentClient.post(name, data, hp)`](#agentclientpost)                                                 | 10      | Ticks waiting for the reply.                                               | Resolves `Err(CommunicationClientAgentError(StreamErr.Timeout))`.                                  |

You never send heartbeats yourself: any received frame (data, ping or pong) refreshes a stream. Use positive integers for every hp value: the countdowns end when they reach exactly 0, so negative or fractional values never expire, and `0` either never expires or, for a connection hp with a socket attached, destroys the connection on the first tick.

### Reconnects and resumption

When the WebSocket of a client connection closes, [`Client.connect()`](#clientconnect) looks at the close code:

- **Final codes** end the connection: 4401, 4403, 4404, 4409 and 4000 (see [WebSocket close codes](#websocket-close-codes)). If `connect()` is still pending it resolves with the matching [`ConnectFail`](#connectfail); otherwise the socket is destroyed and [`waitClose()`](#socket) reports the matching [`CloseCodes`](#closecodes).
- **Any other code** (1006 abnormal closure, 1011, 1008, ...) is treated as transient. The client opens a new WebSocket after `min(30 s, 500 ms * 2^n) * random(0.5 .. 1.5)`, where `n` is the number of consecutive reconnects. `n` resets after a socket has stayed open for 5 seconds and when the endpoint is first accepted, so a server that accepts the upgrade and drops it immediately still sees growing delays.

Limits that end a connection attempt (all optional, see [Client options](#client-options)): `connectTimeout` (milliseconds until the endpoint is accepted), `maxReconnectAttempts` (consecutive reconnects) and `hp` (ticks spent without a socket, 300 by default).

**Resumption.** The new WebSocket is attached to the same [`Connection`](#connection). While the client is offline, writes are queued (up to each stream's cap), `outgoingStream()` calls wait, and incoming stream requests that could not be accepted yet stay queued (within their `streamRequestHp`). As soon as the new socket opens, the connection refills its hp, delivers queued `accept_stream` frames and flushes queued writes on the next tick. On the server, [`@araminix/platform`](#related-packages) keeps the connection for its own hp window, attaches the new socket to it and sends `accept_endpoint` again (which the client ignores).

> [!IMPORTANT]
> The **socket** survives up to `hp` ticks (5 minutes) of cumulative downtime, but each **stream** gives up after about 20 ticks without receiving anything (see the heartbeat rows above). Streams survive short network drops; after a longer outage the socket comes back while the streams that were open have been closed as `Lost`. Re-open them when [`Stream.waitClose()`](#stream) reports `Lost`.

A takeover is final on purpose: when the same client opens the same endpoint and argument elsewhere, the server attaches the new socket and closes the old one with 4409, and the old client stops with [`ConnectFail.Concurrent`](#connectfail) / [`CloseCodes.Concurrent`](#closecodes) instead of fighting back.

### The Result monad

Every fallible API returns a [`Result<T, E>`](#result): either [`Ok<T>`](#ok) holding the value in `v`, or [`Err<E>`](#err) holding the error in `e`. Errors are values (usually enum members such as [`StreamErr.Timeout`](#streamerr)), so they are part of the type and cannot be forgotten, and promises returned by the library never reject.

```ts
import { Err, Ok, Result } from '@araminix/lib';

function parsePort(input: string): Result<number, string> {
    const port = Number(input);
    return Number.isInteger(port) && port > 0 && port < 65536 ? new Ok(port) : new Err(`invalid port: ${input}`);
}

const port = parsePort(process.env.PORT ?? '8080');
if (port.isOk()) {
    console.log('listening on', port.v); // narrowed to Ok<number>
} else {
    console.error(port.e); // narrowed to Err<string>
}
```

- [`isOk()`](#ok) / [`isErr()`](#err) are type guards that narrow the union.
- [`unwrap()`](#err) returns the value of an `Ok` and throws for an `Err`: the error itself if it is an `Error`, otherwise an [`UnwrapError`](#unwraperror) carrying the original value.
- [`serialize()`](#ok) turns a result into `{ ok: true, v }` or `{ ok: false, e }`, the format [`AgentClient`](#agentclient) uses on the wire.
- [`wrap()`](#wrap) and [`wrapSync()`](#wrapsync) convert throwing code into results; [`deal()`](#deal) creates a promise with a one-shot resolver; [`ok()`](#ok-helper), [`err()`](#err-helper) and [`R`](#r) cover the payload-less case.

## Guides

### Browser client with cookie authentication

In browsers, do **not** set [`ClientOptions.token`](#client-options). [`Client.auth()`](#client) logs in and the server answers with an httpOnly session cookie; HTTP calls send it because they use `withCredentials: true`, and the browser sends it with the WebSocket upgrade. Every HTTP call also sends `X-Requested-With: XMLHttpRequest`, which the platform's CSRF guard requires for cookie-authenticated requests. If the page and the API live on different origins, the server must allow credentialed CORS requests and issue a cookie the browser is willing to send.

```ts
import { Client, CloseCodes, ConnectFail, Socket } from '@araminix/lib';

// Same origin as the page: http(s)://host -> ws(s)://host
const client = new Client({
    host: window.location.origin,
    wsHost: window.location.origin.replace(/^http/, 'ws'),
    mount: '/api',
    connectTimeout: 15_000,
});

function showLogin(): void {
    window.location.assign('/login');
}

export async function login(username: string, password: string): Promise<boolean> {
    // POST /api/auth; on success the server sets the session cookie. The body format is defined by the server's auth provider.
    const result = await client.auth({ username, password });
    return result.isOk();
}

export async function openSession(): Promise<Socket | undefined> {
    // Opens wss://<host>/api/ws/platform; the session cookie authenticates the upgrade.
    const connected = await client.connect('platform');
    if (connected.isErr()) {
        switch (connected.e) {
            case ConnectFail.Unauthorized:
                showLogin();
                break;
            case ConnectFail.Concurrent:
                console.warn('This session was opened somewhere else.');
                break;
            default:
                console.error('Cannot connect:', connected.e);
        }
        return undefined;
    }

    const socket = connected.unwrap();
    socket.waitClose().then((reason) => {
        if (reason === CloseCodes.Unauthorized) showLogin();
        else if (reason !== CloseCodes.Intentional) console.warn('Connection lost:', reason);
    });
    window.addEventListener('pagehide', () => socket.destroy());
    return socket;
}

export async function logout(socket: Socket): Promise<void> {
    socket.destroy();
    await client.deauth(); // DELETE /api/auth
}
```

### Node.js agent with a token

In Node.js, pass the token. It is sent as the `x-token` header on the WebSocket upgrade and on every HTTP call, never in the URL. [`Client.connectAgent()`](#client) connects to the `agent` endpoint and returns an [`AgentClient`](#agentclient).

```ts
import { Client, ConnectFail, LOG_LEVEL, logger } from '@araminix/lib';

logger.configure({ level: LOG_LEVEL.INFO, prefix: 'agent' });

const token = process.env.ARAMINIX_TOKEN;
if (!token) {
    throw new Error('ARAMINIX_TOKEN is not set');
}

const client = new Client({
    host: 'https://api.example.com',
    wsHost: 'wss://api.example.com',
    token, // sent as the x-token header
    connectTimeout: 30_000,
    maxReconnectAttempts: 10,
});

const connected = await client.connectAgent();
if (connected.isErr()) {
    logger.error('agent connection failed:', connected.e);
    process.exit(connected.e === ConnectFail.Unauthorized ? 2 : 1);
}
const agent = connected.unwrap();

process.once('SIGTERM', () => agent.destroy());

// Serve tasks and send requests here (see the AgentClient guide), then wait for the end of the connection.
const reason = await agent.waitClose();
logger.warn('agent connection closed:', reason);
```

### Serving incoming streams with loop

[`Socket.incomingStream(name)`](#socketincomingstream) returns one stream per call, so a server-like consumer calls it in a [`loop()`](#loop). Keep in mind:

- Only **one** `incomingStream()` call per name can wait at a time; do not start two loops for the same name.
- A request that is not acquired within `streamRequestHp` ticks (10 s) is dropped. Accept the next stream right away and handle slow work in the background with [`once()`](#once).
- `loop()` never throws. Exceptions are logged through [`logger`](#logger-instance) and passed to the optional `onError`, which decides whether to continue.
- Iterations are chained as microtasks; every iteration must await something real (such as `incomingStream()`) or it would starve the event loop.

```ts
import { loop, LoopResult, once, sleep, Socket, Stream, StreamErr } from '@araminix/lib';

type Quote = { symbol: string; price: number };

async function runCommand(command: string): Promise<string> {
    return `ran ${command}`; // your own work
}

// One stream per command: read the arguments, answer once, close.
export function serveCommands(socket: Socket): void {
    loop(
        async () => {
            const incoming = await socket.incomingStream<{ command: string }, never, string>('command');
            if (incoming.isErr()) return LoopResult.Stop; // socket destroyed, or another acquirer took over
            const stream = incoming.unwrap();
            once(async () => {
                const output = await runCommand(stream.args.command);
                await stream.write(output);
                stream.closeOk();
            });
            return LoopResult.Cont; // go straight back to accepting the next stream
        },
        async () => {
            await sleep(1000); // an iteration threw (already logged): back off, then keep serving
            return LoopResult.Cont;
        },
    );
}

// A long-lived stream: read messages until the peer closes it.
export async function consumeQuotes(stream: Stream<unknown, Quote>): Promise<void> {
    while (true) {
        const quote = await stream.read(60);
        if (quote.isErr()) {
            if (quote.e === StreamErr.Timeout) continue; // a quiet minute; the stream is still open
            break; // Dead: closed and every buffered message has been read
        }
        console.log(quote.unwrap().symbol, quote.unwrap().price);
    }
}
```

### Request/response with AgentClient

[`AgentClient`](#agentclient) adds request/response on top of streams. Replies are serialised results: `{ ok: true, v }` or `{ ok: false, e }`.

- [`post(name, data, hp)`](#agentclientpost) opens a stream called `post` with the arguments `{ name, data }` and waits up to `hp` ticks for the reply. The server's `post` handler (in `@araminix/platform`, the agent's worker) produces the reply and closes the stream.
- [`onTask(name)`](#agentclientontask) waits for the next task the server sends to this agent and hands you its input plus a one-shot `respond` function.

```ts
import { Client, CommunicationClientAgentError, Err, ExecutionClientAgentError, loop, LoopResult, Ok, once, StreamErr } from '@araminix/lib';

const client = new Client({ host: 'https://api.example.com', wsHost: 'wss://api.example.com', token: process.env.ARAMINIX_TOKEN });
const connected = await client.connectAgent();
if (connected.isErr()) throw new Error(`agent connection failed: ${connected.e}`);
const agent = connected.unwrap();

async function resize(url: string, width: number): Promise<{ url: string; width: number }> {
    return { url: `${url}?w=${width}`, width }; // your own work
}

// Serve "resize" tasks from the server.
loop(async () => {
    const task = await agent.onTask<{ url: string; width: number }, { url: string; width: number }, string>('resize');
    if (task.isErr()) return LoopResult.Stop; // SocketErr.Closed or SocketErr.Concurrent
    const [input, respond] = task.unwrap();
    once(async () => {
        try {
            respond(new Ok(await resize(input.url, input.width)));
        } catch (error) {
            respond(new Err(error instanceof Error ? error.message : String(error)));
        }
    });
    return LoopResult.Cont;
});

// Send a request to the server and wait up to 30 ticks for the reply.
const reply = await agent.post<{ text: string }, { summary: string }, string>('summarize', { text: 'Long article ...' }, 30);
if (reply.isOk()) {
    console.log(reply.unwrap().summary);
} else if (reply.e instanceof ExecutionClientAgentError) {
    console.error('the handler failed:', reply.e.e); // the `e` of { ok: false, e }
} else if (reply.e instanceof CommunicationClientAgentError && reply.e.e === StreamErr.Timeout) {
    console.error('no reply within 30 s');
} else {
    console.error('request failed:', reply.e);
}
```

[`AgentClientError`](#agentclienterror) is implemented, not extended, by the two concrete error classes, so test with `instanceof` on [`ExecutionClientAgentError`](#executionclientagenterror) / [`CommunicationClientAgentError`](#communicationclientagenterror) or compare `error.type` with [`AgentClientErrorType`](#agentclienterrortype).

### Stream backpressure and queue caps

There is no end-to-end flow control: [`Stream.write()`](#streamwrite) resolves as soon as the frame was handed to the WebSocket. What the library does provide is two bounded queues per stream, with fail-fast overflow.

**Outgoing.** A write is sent immediately when the socket is open, nothing is queued ahead of it and the heartbeat is healthy. Otherwise it is queued and its promise stays pending until it is sent (or the stream dies). The queue is flushed in order on every tick, whenever a frame arrives and before the next write. If a write finds the queue already holding [`outgoingQueueCap`](#stream) messages (default 256), the stream is closed with [`StreamCloseCodes.Error`](#streamclosecodes) and every pending write resolves [`StreamErr.Dead`](#streamerr).

- Awaiting each write is the simplest backpressure: while offline, the producer waits instead of filling the queue.
- If you fire writes without awaiting them, watch [`outgoingQueueLength`](#stream) and pause well below the cap.
- [`closeOk()`](#stream) discards queued writes; await them first if they must be delivered.
- Payloads must be JSON-serialisable. A value `JSON.stringify` rejects (circular structure, BigInt) is never sent and blocks the queue.

**Incoming.** A message is handed to the waiting [`read()`](#streamread) or, if none is waiting, buffered. When a message arrives while [`incomingQueueCap`](#stream) messages (default 256) are already buffered, the stream is closed with `StreamCloseCodes.Error`; the buffered messages remain readable. Keep a read pending on busy streams, and raise the cap right after obtaining the stream if the peer sends bursts.

```ts
import { Result, sleep, Stream, StreamErr } from '@araminix/lib';

type Row = { id: number; value: string };

// Producer: fire-and-forget writes with a high-water mark, for maximum throughput.
export async function sendRows(stream: Stream<unknown, unknown, Row>, rows: Iterable<Row>): Promise<boolean> {
    stream.outgoingQueueCap = 1024; // allow a longer offline buffer for this stream
    const pending: Promise<Result<void, StreamErr>>[] = [];
    for (const row of rows) {
        while (stream.active && stream.outgoingQueueLength >= 512) {
            await sleep(200); // wait for the reconnect (or the heartbeat) to drain the queue
        }
        if (!stream.active) return false;
        pending.push(stream.write(row));
    }
    const results = await Promise.all(pending);
    return results.every((result) => result.isOk());
}

// Consumer: accept bursts of up to 2048 unread messages.
export function prepareConsumer(stream: Stream<unknown, Row>): void {
    stream.incomingQueueCap = 2048;
}
```

### Error handling with Result and UnwrapError

Handle errors where they occur by narrowing, and switch over the error enums ([`ConnectFail`](#connectfail), [`SocketErr`](#socketerr), [`StreamErr`](#streamerr), [`InitStreamErr`](#initstreamerr)). Reach for [`unwrap()`](#err) when a failure really is exceptional; since 3.0.0 it always throws an `Error`: non-`Error` values are wrapped in an [`UnwrapError`](#unwraperror) whose `e` (and `cause`) hold the original value.

```ts
import { Client, ConnectFail, InitStreamErr, StreamErr, UnwrapError, wrap, wrapSync } from '@araminix/lib';

const client = new Client({ host: 'https://api.example.com', wsHost: 'wss://api.example.com' });

// 1. Narrow and switch.
const connected = await client.connect('platform');
if (connected.isErr()) {
    const messages: Record<ConnectFail, string> = {
        [ConnectFail.Unauthorized]: 'Please log in.',
        [ConnectFail.Denied]: 'Access denied.',
        [ConnectFail.NotFound]: 'Unknown endpoint.',
        [ConnectFail.Concurrent]: 'Opened in another window.',
        [ConnectFail.Unexpected]: 'Connection failed, try again later.',
    };
    console.error(messages[connected.e]);
} else {
    const socket = connected.unwrap(); // safe: narrowed to Ok
    const opened = await socket.outgoingStream('report', { month: '2026-10' });
    if (opened.isErr() && opened.e === InitStreamErr.Dead) {
        console.error('the server did not accept the stream');
    } else if (opened.isOk()) {
        const first = await opened.unwrap().read(15);
        if (first.isErr() && first.e === StreamErr.Timeout) console.warn('slow server');
    }
}

// 2. unwrap() and UnwrapError.
try {
    const socket = (await client.connect('platform')).unwrap();
    socket.destroy();
} catch (error) {
    if (error instanceof UnwrapError) {
        console.error('connect failed with', error.e); // e.g. 'Unauthorized'; error.cause holds the same value
    } else {
        throw error;
    }
}

// 3. Bring throwing APIs into the same style.
const health = await wrap(fetch('https://api.example.com/health'));
if (health.isErr()) console.error('network error:', health.e.message);
const config = wrapSync(() => JSON.parse('{"debug": true}') as { debug: boolean });
if (config.isOk()) console.log('debug mode:', config.v.debug);
```

### Logging and file storage

The library logs through the shared [`logger`](#logger-instance) instance: WebSocket errors and closes, invalid frames, errors caught by `loop()`/`once()` and, at `TRACE`, every incoming frame. The default level is `DEBUG`; priorities are `TRACE` < `DEBUG` < `INFO` < `WARN` < `ERROR`, and `WARN`/`ERROR` go to `console.error`.

```ts
import { LOG_LEVEL, Logger, logger } from '@araminix/lib';

// Tune the library's own output.
logger.configure({ level: LOG_LEVEL.INFO, format: 'json', prefix: 'agent' });

// Node.js only: also write INFO and above to daily files in ./logs (created if missing):
//   ./logs/agent-logs-YYYY-MM-DD.log    every stored level except ERROR
//   ./logs/agent-errors-YYYY-MM-DD.log  ERROR
logger.initStore({ directory: './logs', level: LOG_LEVEL.INFO, filePrefix: 'agent' });

// A separate logger for your own module.
const log = new Logger({ level: LOG_LEVEL.DEBUG, showStackTrace: true, showTimestamp: true, format: 'text', prefix: 'billing' });
log.info('invoice sent', { id: 42 });
log.error('payment failed:', new Error('card declined'));

// Temporarily see every frame the connections receive.
logger.setLevel(LOG_LEVEL.TRACE);
```

File storage is optional and Node.js only. `node:fs` and `node:path` are imported lazily the first time a line is stored, lines are appended in order through write streams in the background, and file problems are reported with `console.error` instead of throwing. File names use the UTC date, so files roll over at UTC midnight; stored lines always look like `[timestamp] [LEVEL] message`, whatever the console format. The default file prefix is `folkrum`.

## API reference

Symbols are grouped by source module. Everything listed here is exported from the package entry point, [index.ts](index.ts). The stream controllers in [src/stream.ts](src/stream.ts) (`StreamController`, `IncomingStreamController`, `OutgoingStreamController`) are internal and not exported.

### Client and agent

#### `Client`

Source: [src/client.ts](src/client.ts)

```ts
declare class Client {
    constructor(options: ClientOptions);
    connect(name: string, arg?: string): Promise<Result<Socket, ConnectFail>>;
    connectAgent(): Promise<Result<AgentClient, ConnectFail>>;
    auth<TAuthData = unknown, TAuthResponse = unknown, TErr = unknown>(data: TAuthData): Promise<Result<AxiosResponse<TAuthResponse>, TErr>>;
    gauth<TResponse = unknown, TErr = unknown>(access_token: string): Promise<Result<AxiosResponse<TResponse>, TErr | void>>;
    deauth(): Promise<Ok<void> | Err<void, void>>;
    request2FA<T = unknown, E = unknown>(): Promise<Result<AxiosResponse<T>, E>>;
    enable2FA<T = unknown, E = unknown>(secret: string, code: string): Promise<Result<AxiosResponse<T>, E>>;
    verify2FA<T = unknown, E = unknown>(accountName: string, code: string, uuid: string): Promise<Result<AxiosResponse<T>, E>>;
    disable2FA<T = unknown, E = unknown>(code?: string): Promise<Result<AxiosResponse<T>, E>>;
    upload(file: string | Blob): Promise<Result<FileInfo, void>>;
    getFileLink(file: string | FileInfo): string;
    getPresignedUploadUrl<T = unknown, E = unknown>(mimetype?: string, fileName?: string): Promise<Result<AxiosResponse<T>, E>>;
    getPresignedDownloadUrl<T = unknown, E = unknown>(fileId: string): Promise<Result<AxiosResponse<T>, E>>;
}
```

Entry point for talking to an [`@araminix/platform`](#related-packages) server (or any server speaking the same protocol): WebSocket endpoints plus HTTP helpers for authentication, two-factor authentication and files. The constructor only stores the options; nothing is requested until you call a method.

| Constructor parameter | Type                              | Default  | Description                                                                             |
| --------------------- | --------------------------------- | -------- | --------------------------------------------------------------------------------------- |
| `options`             | [`ClientOptions`](#clientoptions) | required | Hosts, mount prefix, token and reconnect limits; see [Client options](#client-options). |

**HTTP helpers.** All of them go through [`axiosService`](#axiosservice), send `X-Requested-With: XMLHttpRequest` (plus `x-token` when a token is set) with `withCredentials: true`, and resolve to a [`Result`](#result) instead of throwing. Unless noted, `Ok` holds the full `AxiosResponse` and `Err` holds what axios threw (an `AxiosError` for non-2xx statuses).

| Method                                        | Request                                  | Body                                     | Notes                                                                                                                               |
| --------------------------------------------- | ---------------------------------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `auth(data)`                                  | `POST <host><mount>/auth`                | `data`                                   | Logs in; a platform server sets the httpOnly session cookie (or answers with a body, e.g. for 2FA).                                 |
| `gauth(access_token)`                         | `POST <host><mount>/gauth`               | `{ access_token }`                       | Google login. Resolves `Err(undefined)` without a request when `access_token` is empty.                                             |
| `deauth()`                                    | `DELETE <host><mount>/auth`              | none                                     | Logs out. Resolves `ok()` or `err()`; error details are discarded.                                                                  |
| `request2FA()`                                | `POST <host><mount>/2fa/request`         | none                                     | Starts 2FA enrolment; a platform server answers `{ url }`.                                                                          |
| `enable2FA(secret, code)`                     | `POST <host><mount>/2fa/enable`          | `{ secret, code }`                       | Confirms enrolment with a one-time code.                                                                                            |
| `verify2FA(accountName, code, uuid)`          | `POST <host><mount>/2fa/verify`          | `{ accountName, code, uuid }`            | Second login step; `uuid` is the identifier your server's auth provider expects.                                                    |
| `disable2FA(code?)`                           | `DELETE <host><mount>/2fa/disable`       | `{ code }` if `code` is given, else none | Turns 2FA off.                                                                                                                      |
| `upload(file)`                                | `POST <host><mount>/file`                | `multipart/form-data`, field `file`      | Resolves `Ok(info)` with a [`FileInfo`](#fileinfo), or `Err(undefined)`. Pass a `Blob`/`File`; a string becomes a plain text field. |
| `getFileLink(file)`                           | none (builds a URL)                      | none                                     | `<host><mount>/file/<id>` for an id (not encoded), `<host><mount>/file/<id>/<encoded name>` for a `FileInfo`.                       |
| `getPresignedUploadUrl(mimetype?, fileName?)` | `POST <host><mount>/attachments/upload`  | `{ mimetype, fileName }`                 | Presigned URL for a direct upload to object storage.                                                                                |
| `getPresignedDownloadUrl(fileId)`             | `GET <host><mount>/attachments/<fileId>` | none                                     | `fileId` is URL-encoded, so keys containing `/` work.                                                                               |

##### `Client.connect()`

```ts
connect(name: string, arg?: string): Promise<Result<Socket, ConnectFail>>
```

Opens the WebSocket endpoint `${wsHost}${mount}/ws/<name>[/<arg>]` and resolves once the server sends `accept_endpoint`. `name` and `arg` are inserted as-is; encode them if they may contain reserved characters. In Node.js the token travels as the `x-token` header; browsers rely on the session cookie. The returned [`Socket`](#socket) stays usable across network drops (see [Reconnects and resumption](#reconnects-and-resumption)).

| Parameter | Type     | Default     | Description                                                                          |
| --------- | -------- | ----------- | ------------------------------------------------------------------------------------ |
| `name`    | `string` | required    | Endpoint name, e.g. `'platform'` or `'agent'`.                                       |
| `arg`     | `string` | `undefined` | Optional endpoint argument (e.g. a document id); exposed as [`Socket.arg`](#socket). |

| Close code    | `connect()` result (while pending) | Connection closed with    |
| ------------- | ---------------------------------- | ------------------------- |
| 4401          | `Err(ConnectFail.Unauthorized)`    | `CloseCodes.Unauthorized` |
| 4403          | `Err(ConnectFail.Denied)`          | `CloseCodes.Unauthorized` |
| 4404          | `Err(ConnectFail.NotFound)`        | `CloseCodes.Unexpected`   |
| 4409          | `Err(ConnectFail.Concurrent)`      | `CloseCodes.Concurrent`   |
| 4000          | `Err(ConnectFail.Unexpected)`      | `CloseCodes.Intentional`  |
| anything else | reconnect with backoff             | (stays open)              |

**Returns** `Ok(socket)`, or `Err` with a [`ConnectFail`](#connectfail). `ConnectFail.Unexpected` also covers the connect timeout, the reconnect limit, the connection hp running out and a missing global `WebSocket`. Never rejects.

```ts
const connected = await client.connect('documents', 'doc-42'); // wss://.../ws/documents/doc-42
if (connected.isErr()) {
    console.error('cannot open the document:', connected.e);
} else {
    const socket = connected.unwrap();
    console.log('connected to', socket.arg); // 'doc-42'
}
```

[`connectAgent()`](#client) is `connect('agent')` with the socket wrapped in an [`AgentClient`](#agentclient).

#### `ClientOptions`

Source: [src/client.ts](src/client.ts)

```ts
interface ClientOptions {
    host: string;
    wsHost: string;
    mount?: string;
    token?: string;
    hp?: number;
    connectTimeout?: number;
    maxReconnectAttempts?: number;
}
```

Options of the [`Client`](#client) constructor. Every field, with its type, default and meaning, is listed under [Client options](#client-options).

```ts
const client = new Client({ host: 'https://api.example.com', wsHost: 'wss://api.example.com', mount: '/api', connectTimeout: 10_000, maxReconnectAttempts: 8 });
```

#### `ConnectFail`

Source: [src/client.ts](src/client.ts)

```ts
enum ConnectFail {
    Unexpected = 'Unexpected',
    Unauthorized = 'Unauthorized',
    NotFound = 'NotFound',
    Denied = 'Denied',
    Concurrent = 'Concurrent',
}
```

Why [`Client.connect()`](#clientconnect) or `connectAgent()` failed. Member meanings are listed under [Connect failures](#connect-failures).

```ts
const connected = await client.connect('platform');
if (connected.isErr() && connected.e === ConnectFail.Unauthorized) {
    window.location.assign('/login');
}
```

#### `FileInfo`

Source: [src/client.ts](src/client.ts)

```ts
interface FileInfo {
    name: string;
    size: number;
    mimetype: string;
    id?: string;
    userId: string;
}
```

Metadata of an uploaded file, returned by [`Client.upload()`](#client) (the JSON body of `POST <host><mount>/file`).

| Field      | Type     | Description                                                         |
| ---------- | -------- | ------------------------------------------------------------------- |
| `name`     | `string` | Original file name, as stored by the server.                        |
| `size`     | `number` | Size in bytes.                                                      |
| `mimetype` | `string` | MIME type.                                                          |
| `id`       | `string` | Optional. File id assigned by the server; `getFileLink()` needs it. |
| `userId`   | `string` | Id of the uploading user.                                           |

```ts
const uploaded = await client.upload(new Blob(['hello'], { type: 'text/plain' }));
if (uploaded.isOk()) {
    const info: FileInfo = uploaded.unwrap();
    console.log(`${info.name}: ${info.size} bytes at ${client.getFileLink(info)}`);
}
```

#### `AgentClient`

Source: [src/client.ts](src/client.ts)

```ts
declare class AgentClient {
    constructor(socket: Socket);
    onTask<TTask = unknown, TResult = unknown, TError = unknown>(name: string): Promise<Result<[TTask, (value: Result<TResult, TError>) => R], SocketErr>>;
    post<TData = unknown, TResult = unknown, E = unknown>(name: string, data: TData, hp?: number): Promise<Result<TResult, AgentClientError>>;
    waitClose(): Promise<CloseCodes>;
    destroy(): void;
}
```

Request/response helper for the `agent` endpoint, usually obtained from `Client.connectAgent()`. Replies travel as serialised results (`{ ok: true, v }` or `{ ok: false, e }`).

| Member                  | Description                                                                                   |
| ----------------------- | --------------------------------------------------------------------------------------------- |
| `constructor(socket)`   | Wraps a [`Socket`](#socket) connected to the `agent` endpoint.                                |
| `onTask(name)`          | Waits for the next task called `name`; see [below](#agentclientontask).                       |
| `post(name, data, hp?)` | Sends a request and waits for the reply; see [below](#agentclientpost).                       |
| `waitClose()`           | Resolves with the [`CloseCodes`](#closecodes) reason once the connection is destroyed.        |
| `destroy()`             | Closes the connection for good ([`CloseCodes.Intentional`](#closecodes)); no more reconnects. |

##### `AgentClient.onTask()`

```ts
onTask<TTask, TResult, TError>(name: string): Promise<Result<[TTask, (value: Result<TResult, TError>) => R], SocketErr>>
```

Accepts the next incoming stream called `name` (via [`Socket.incomingStream()`](#socketincomingstream)); its `args` are the task input. Calling `respond(result)` writes `result.serialize()` to the stream in the background; the requester reads it and closes the stream. Only the first call counts: `respond` returns `ok()` once and `err()` afterwards. Tasks wait at most `streamRequestHp` ticks (10 s) for an `onTask()` call, so ask for the next task right away and handle slow tasks with [`once()`](#once).

| Parameter / type parameter | Type     | Description                                 |
| -------------------------- | -------- | ------------------------------------------- |
| `name`                     | `string` | Task (stream) name.                         |
| `TTask`                    | type     | Task input type (not validated at runtime). |
| `TResult`, `TError`        | types    | Types of the success and error answers.     |

**Returns** `Ok([input, respond])`, or `Err(SocketErr.Closed)` when the connection is dead and `Err(SocketErr.Concurrent)` when a newer call for the same name took over. If `respond` is never called, the requester's read times out.

```ts
const task = await agent.onTask<{ a: number; b: number }, number, string>('add');
if (task.isOk()) {
    const [input, respond] = task.unwrap();
    const sum = input.a + input.b;
    respond(Number.isFinite(sum) ? new Ok(sum) : new Err('invalid input'));
}
```

##### `AgentClient.post()`

```ts
post<TData, TResult, E>(name: string, data: TData, hp?: number): Promise<Result<TResult, AgentClientError>>
```

Opens an outgoing stream named `post` with the arguments `{ name, data }` and reads one reply. `{ ok: true, v }` becomes `Ok(v)`; `{ ok: false, e }` becomes `Err(new ExecutionClientAgentError(e))`; a failure to open the stream or to read the reply, or a reply without an `ok` field, becomes `Err(new CommunicationClientAgentError(cause))`. The client does not close the `post` stream itself; the server closes it after replying.

| Parameter | Type     | Default  | Description                                                                                              |
| --------- | -------- | -------- | -------------------------------------------------------------------------------------------------------- |
| `name`    | `string` | required | Request name, interpreted by the server's `post` handler.                                                |
| `data`    | `TData`  | required | JSON-serialisable payload.                                                                               |
| `hp`      | `number` | `10`     | Reply timeout in ticks (seconds), after the stream was opened (opening itself may take up to ~20 ticks). |

**Returns** `Ok(value)`, or `Err(error)` with an [`AgentClientError`](#agentclienterror). Never rejects.

```ts
const reply = await agent.post<{ id: number }, { name: string }, string>('lookup', { id: 7 }, 15);
if (reply.isOk()) console.log(reply.unwrap().name);
```

#### `AgentClientError`

Source: [src/client.ts](src/client.ts)

```ts
abstract class AgentClientError {
    abstract type: AgentClientErrorType;
}
```

Common type of the errors returned by [`AgentClient.post()`](#agentclientpost). The two concrete classes, [`CommunicationClientAgentError`](#communicationclientagenterror) and [`ExecutionClientAgentError`](#executionclientagenterror), _implement_ it rather than extend it, so `instanceof AgentClientError` is always `false`; check `type` or use `instanceof` with a concrete class.

```ts
if (reply.isErr()) {
    console.error(reply.e.type === AgentClientErrorType.Execution ? 'handler error' : 'communication error');
}
```

#### `AgentClientErrorType`

Source: [src/client.ts](src/client.ts)

```ts
enum AgentClientErrorType {
    Communication = 'Communication',
    Execution = 'Execution',
}
```

Discriminator stored in [`AgentClientError.type`](#agentclienterror). `Communication`: the exchange did not complete (stream not opened, no valid reply in time, stream closed). `Execution`: the server handled the request and answered with an error. See [Agent errors](#agent-errors).

```ts
const failedRemotely = reply.isErr() && reply.e.type === AgentClientErrorType.Execution;
```

#### `CommunicationClientAgentError`

Source: [src/client.ts](src/client.ts)

```ts
declare class CommunicationClientAgentError<E = unknown> implements AgentClientError {
    readonly e: E;
    readonly type: AgentClientErrorType.Communication;
    constructor(e: E);
}
```

[`AgentClient.post()`](#agentclientpost) could not complete the exchange. `e` is the cause: an [`InitStreamErr`](#initstreamerr) (the `post` stream could not be opened), a [`StreamErr`](#streamerr) (`Timeout`: no reply within `hp` ticks; `Dead`: the stream closed without a reply), or the string `'Format'` (the reply was not an object with an `ok` field).

```ts
if (reply.isErr() && reply.e instanceof CommunicationClientAgentError && reply.e.e === StreamErr.Timeout) {
    console.warn('no reply in time');
}
```

#### `ExecutionClientAgentError`

Source: [src/client.ts](src/client.ts)

```ts
declare class ExecutionClientAgentError<E = unknown> implements AgentClientError {
    readonly e: E;
    readonly type: AgentClientErrorType.Execution;
    constructor(e: E);
}
```

The server handled an [`AgentClient.post()`](#agentclientpost) request and replied `{ ok: false, e }`; `e` is that error value (for example what a handler passed to `respond(new Err(...))` in [`onTask()`](#agentclientontask)).

```ts
if (reply.isErr() && reply.e instanceof ExecutionClientAgentError) {
    console.error('the server reported:', reply.e.e);
}
```

#### `axiosService`

Source: [src/client.ts](src/client.ts)

```ts
declare const axiosService: AxiosStatic; // the axios default instance
```

The axios default instance that every [`Client`](#client) HTTP helper uses. Configure defaults or interceptors on it without importing axios yourself. Changes affect all clients in the process (and any other code using the axios default instance).

```ts
import { axiosService } from '@araminix/lib';

axiosService.defaults.timeout = 15_000;
axiosService.interceptors.response.use(undefined, (error) => {
    if (error.response?.status === 401) window.location.assign('/login');
    return Promise.reject(error);
});
```

### Sockets

#### `Socket`

Source: [src/Socket.ts](src/Socket.ts)

```ts
declare class Socket {
    connection: Connection;
    constructor(connection: Connection);
    readonly arg: string;
    incomingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>>;
    outgoingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string, args?: TArgs): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>>;
    waitClose(): Promise<CloseCodes>;
    isDead(): boolean;
    destroy(): void;
    getConnectionStatus(): ConnectionStatus;
}
```

The application's handle on one accepted endpoint connection; a thin wrapper around a [`Connection`](#connection). Clients get it from [`Client.connect()`](#clientconnect); servers wrap their own connections with `new Socket(connection)`.

| Member                        | Description                                                                                                                                                                                                   |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection`                  | The underlying [`Connection`](#connection), for advanced use.                                                                                                                                                 |
| `arg`                         | The endpoint argument passed to `connect(name, arg)`, or `''`.                                                                                                                                                |
| `incomingStream(name)`        | Waits for a stream the peer opens; see [below](#socketincomingstream).                                                                                                                                        |
| `outgoingStream(name, args?)` | Opens a stream towards the peer; see [below](#socketoutgoingstream).                                                                                                                                          |
| `waitClose()`                 | Resolves with the [`CloseCodes`](#closecodes) reason once the connection is destroyed (immediately if it already is). A server-side destroy arrives as close code 4000 and is reported as `Intentional`.      |
| `isDead()`                    | `true` once the connection was destroyed; `false` otherwise, including while reconnecting.                                                                                                                    |
| `destroy()`                   | Destroys the connection with `CloseCodes.Intentional`: streams close with `Lost`, waiting `incomingStream()` calls get `SocketErr.Closed`, the WebSocket is closed with 4000, reconnecting stops. Idempotent. |
| `getConnectionStatus()`       | The [`ConnectionStatus`](#connectionstatus) of the attached WebSocket, or `Unknown` when none is attached. Never throws.                                                                                      |

```ts
once(async () => {
    const reason = await socket.waitClose();
    console.log('socket closed:', reason, 'status:', socket.getConnectionStatus());
});
```

##### `Socket.incomingStream()`

```ts
incomingStream<TArgs, TDataIn, TDataOut>(name: string): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>>
```

Waits for the peer to open a stream called `name` and resolves once it is accepted. A queued request is accepted as soon as `accept_stream` can be sent; otherwise the call waits for the next one.

| Parameter / type parameter | Type     | Description                                                  |
| -------------------------- | -------- | ------------------------------------------------------------ |
| `name`                     | `string` | The name the peer passes to its `outgoingStream()`.          |
| `TArgs`                    | type     | Type of [`Stream.args`](#stream) (not validated at runtime). |
| `TDataIn`, `TDataOut`      | types    | Types of the messages you read and write.                    |

**Returns** `Ok(stream)`; `Err(SocketErr.Closed)` when the connection is or becomes dead; `Err(SocketErr.Concurrent)` when a newer call for the same name replaced this one. Requests nobody acquires within `streamRequestHp` ticks (10) are dropped, so serve names continuously with [`loop()`](#loop).

```ts
const incoming = await socket.incomingStream<{ sensorId: string }, number>('telemetry');
if (incoming.isOk()) {
    const stream = incoming.unwrap();
    const reading = await stream.read();
    if (reading.isOk()) console.log(stream.args.sensorId, reading.unwrap());
}
```

##### `Socket.outgoingStream()`

```ts
outgoingStream<TArgs, TDataIn, TDataOut>(name: string, args?: TArgs): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>>
```

Sends `new_stream` with `args` and resolves when the peer accepts. If the socket cannot send yet (for example during a reconnect) the request is retried on every tick. The peer must accept within 20 ticks and acquire the name before its own `streamRequestHp` runs out.

| Parameter | Type     | Default     | Description                                                         |
| --------- | -------- | ----------- | ------------------------------------------------------------------- |
| `name`    | `string` | required    | Stream name; the peer accepts it with `incomingStream(name)`.       |
| `args`    | `TArgs`  | `undefined` | JSON-serialisable arguments, delivered as [`Stream.args`](#stream). |

**Returns** `Ok(stream)` once accepted, or `Err(InitStreamErr.Dead)` when the connection is dead or the stream was rejected, closed or not accepted in time. Never rejects.

```ts
const opened = await socket.outgoingStream<{ room: string }, string, string>('chat', { room: 'general' });
if (opened.isOk()) {
    const stream = opened.unwrap();
    await stream.write('hello');
    const reply = await stream.read(10);
    stream.closeOk();
}
```

#### `SocketErr`

Source: [src/Socket.ts](src/Socket.ts)

```ts
enum SocketErr {
    Closed = 0,
    Concurrent = 1,
}
```

Why [`Socket.incomingStream()`](#socketincomingstream), [`Connection.acquireStream()`](#connection) or [`AgentClient.onTask()`](#agentclientontask) did not deliver a stream. Note that, unlike the other enums, its values are numbers. See [Stream and socket errors](#stream-and-socket-errors).

```ts
const incoming = await socket.incomingStream('jobs');
if (incoming.isErr() && incoming.e === SocketErr.Closed) console.log('socket is gone');
```

#### `ConnectionStatus`

Source: [src/Socket.ts](src/Socket.ts)

```ts
enum ConnectionStatus {
    Connecting = 'connecting',
    Open = 'open',
    Closing = 'closing',
    Closed = 'closed',
    Unknown = 'unknown',
}
```

Returned by [`Socket.getConnectionStatus()`](#socket); mirrors `WebSocket.readyState` 0 to 3. `Unknown` means no WebSocket is attached (e.g. while waiting to reconnect) or its state is unknown. `Closed` is usually short-lived, because a closed WebSocket is detached as soon as its close event is handled.

| Value          | `readyState` | Meaning                                       |
| -------------- | ------------ | --------------------------------------------- |
| `'connecting'` | 0            | Opening, e.g. during a reconnect attempt.     |
| `'open'`       | 1            | Open; frames are sent immediately.            |
| `'closing'`    | 2            | Closing handshake in progress.                |
| `'closed'`     | 3            | Closed (about to be detached).                |
| `'unknown'`    | none         | No socket attached, or an unrecognised state. |

```ts
const online = socket.getConnectionStatus() === ConnectionStatus.Open;
```

### Connections

#### `Connection`

Source: [src/connection.ts](src/connection.ts)

```ts
declare class Connection {
    constructor(inter: ConnectionInterface, arg: string);
    arg: string;
    ws: MinimalWebSocket | undefined;
    dead: boolean;
    readonly closePromise: Promise<CloseCodes>;
    connect(ws: MinimalWebSocket, reset?: boolean): void;
    accept(): R;
    deny(): void;
    acquireStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, SocketErr>>;
    newOutgoingStream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown>(name: string, args: TArgs): Promise<Result<Stream<TArgs, TDataIn, TDataOut>, InitStreamErr>>;
    destroy(closeCode: CloseCodes): void;
    removeStream(id: string): void;
    writeAcceptStream(streamId: string): R;
    writeNewStream<TArgs = unknown>(streamId: string, name: string, args: TArgs): R;
    writeData<T = unknown>(streamId: string, data: T): R;
    writeClose(streamId: string, code: StreamCloseCodes): R;
    writePing(streamId: string): R;
    writePong(streamId: string): R;
}
```

One logical endpoint connection: many streams multiplexed over a WebSocket that can be replaced. It outlives individual WebSockets (see [Reconnects and resumption](#reconnects-and-resumption)) and runs the 1-second tick that drives every hp countdown (see [Heartbeats and hp](#heartbeats-and-hp)). Clients rarely touch it directly; servers create one per endpoint client.

| Constructor parameter | Type                                          | Description                                                                |
| --------------------- | --------------------------------------------- | -------------------------------------------------------------------------- |
| `inter`               | [`ConnectionInterface`](#connectioninterface) | Owner callbacks and limits; see [Connection options](#connection-options). |
| `arg`                 | `string`                                      | The endpoint argument, exposed as `arg` and `Socket.arg`.                  |

| Member                          | Description                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connect(ws, reset = true)`     | Attaches a WebSocket, closing the previous one with 4409. Assigns its `on*` handlers: `open` refills hp, accepts queued requests and calls `onOpen`; `close` detaches it and calls `onDisconnect(code)`; `message` parses and dispatches JSON frames. With `reset`, hp is refilled immediately. On a dead connection the socket is closed with 4000. |
| `accept()`                      | Server side: sends `accept_endpoint`. Returns `ok()`, or `err()` when dead or when sending failed (the connection is then destroyed with `Unexpected`).                                                                                                                                                                                              |
| `deny()`                        | Server side: closes the socket with 4403 and destroys the connection with `CloseCodes.Unauthorized`.                                                                                                                                                                                                                                                 |
| `acquireStream(name)`           | Implementation of [`Socket.incomingStream()`](#socketincomingstream).                                                                                                                                                                                                                                                                                |
| `newOutgoingStream(name, args)` | Implementation of [`Socket.outgoingStream()`](#socketoutgoingstream).                                                                                                                                                                                                                                                                                |
| `destroy(closeCode)`            | Idempotent teardown: marks the connection dead, resolves `closePromise`, stops the timer, closes streams and queued requests with `Lost`, closes the socket with 4000, fails waiting acquirers with `SocketErr.Closed` and calls `onDestroy`.                                                                                                        |
| `closePromise`                  | Resolves with the [`CloseCodes`](#closecodes) reason on destroy. Never rejects.                                                                                                                                                                                                                                                                      |
| `dead`, `ws`, `arg`             | Whether it was destroyed; the attached WebSocket (or `undefined`); the endpoint argument.                                                                                                                                                                                                                                                            |
| `write*(...)`                   | Low-level frame writers. Each returns `ok()` when the frame was handed to `ws.send()`, and `err()` when the payload is not JSON-serialisable, no socket is attached, `readyState` is not 1, or `send()` threw (the socket is then closed so the disconnect path runs).                                                                               |
| `removeStream(id)`              | Used by stream controllers to unregister themselves.                                                                                                                                                                                                                                                                                                 |

```ts
import { Connection, Socket } from '@araminix/lib';

// Server side, `ws` being the WebSocket of an accepted HTTP upgrade:
const connection = new Connection(
    {
        maxPendingStreams: 64,
        onAccept: () => {},
        onDisconnect: (code) => console.log('socket closed with', code),
        onDestroy: () => console.log('connection destroyed'),
    },
    'session-1',
);
connection.connect(ws);
if (connection.accept().isOk()) {
    const socket = new Socket(connection);
}
```

#### `ConnectionInterface`

Source: [src/connection.ts](src/connection.ts)

```ts
interface ConnectionInterface {
    hp?: number;
    streamRequestHp?: number;
    maxPendingStreams?: number;
    onDestroy: () => void;
    onDisconnect: (code: number) => void;
    onAccept: () => void;
    onOpen?: () => void;
}
```

Callbacks and limits supplied by the owner of a [`Connection`](#connection). [`Client.connect()`](#clientconnect) builds one internally (only `hp` is configurable there, via [`ClientOptions.hp`](#client-options)); servers build their own. All fields are described under [Connection options](#connection-options).

```ts
const connection = new Connection({ hp: 120, streamRequestHp: 15, onDestroy: () => {}, onDisconnect: () => {}, onAccept: () => {} }, '');
```

#### `MinimalWebSocket`

Source: [src/connection.ts](src/connection.ts)

```ts
interface MinimalWebSocket {
    send(data: string): void;
    close(code?: number, reason?: string): void;
    onopen: ((event: any) => void) | null;
    onerror: ((event: any) => void) | null;
    onclose: ((event: { code: number; reason?: string }) => void) | null;
    onmessage: ((event: { data: any }) => void) | null;
    readyState?: number;
}
```

The subset of the WHATWG `WebSocket` that a [`Connection`](#connection) needs. The global `WebSocket` of browsers and Node.js and the `ws` package's `WebSocket` all satisfy it. `Connection.connect()` assigns the four handlers. Incoming `data` must be a JSON string or have a `toString()` that yields one (e.g. a Node `Buffer`). When `readyState` is missing, the socket is treated as open.

| Member       | Description                                                                     |
| ------------ | ------------------------------------------------------------------------------- |
| `send`       | Sends a text frame; may throw (the connection then closes the socket).          |
| `close`      | Closes with an optional code (see [`WsCloseCodes`](#wsclosecodes)) and reason.  |
| `onopen`     | Assigned by the connection: refills hp, delivers queued work, calls `onOpen`.   |
| `onerror`    | Assigned by the connection: logs the error.                                     |
| `onclose`    | Assigned by the connection: detaches the socket and calls `onDisconnect(code)`. |
| `onmessage`  | Assigned by the connection: parses and dispatches the frame.                    |
| `readyState` | Optional WHATWG ready state (0 connecting, 1 open, 2 closing, 3 closed).        |

```ts
const sent: string[] = [];
const fake: MinimalWebSocket = { readyState: 1, send: (data) => sent.push(data), close: () => {}, onopen: null, onerror: null, onclose: null, onmessage: null };
connection.connect(fake);
```

#### `WsCloseCodes`

Source: [src/connection.ts](src/connection.ts)

```ts
enum WsCloseCodes {
    InternalError = 1011,
    NotFound = 4404,
    ConcurrentConnection = 4409,
    Unauthorized = 4401,
    Denied = 4403,
    Normal = 4000,
}
```

WebSocket close codes with a meaning in this protocol; see [WebSocket close codes](#websocket-close-codes) for how the client reacts to each.

```ts
// Server side: refuse an unauthenticated upgrade.
ws.close(WsCloseCodes.Unauthorized);
```

#### `CloseCodes`

Source: [src/connection.ts](src/connection.ts)

```ts
enum CloseCodes {
    Died = 'Died',
    Unexpected = 'Unexpected',
    Intentional = 'Intentional',
    Concurrent = 'Concurrent',
    Unauthorized = 'Unauthorized',
}
```

Why a [`Connection`](#connection) was destroyed: the value of `closePromise`, [`Socket.waitClose()`](#socket) and [`AgentClient.waitClose()`](#agentclient). See [Connection close reasons](#connection-close-reasons).

```ts
const reason = await socket.waitClose();
if (reason === CloseCodes.Died) console.warn('offline for too long');
```

### Streams

#### `Stream`

Source: [src/stream.ts](src/stream.ts)

```ts
declare class Stream<TArgs = unknown, TDataIn = unknown, TDataOut = unknown> {
    readonly id: string;
    readonly args: Readonly<TArgs>;
    readonly active: boolean;
    readonly closeCode: StreamCloseCodes | undefined;
    incomingQueueCap: number;
    outgoingQueueCap: number;
    readonly incomingQueueLength: number;
    readonly outgoingQueueLength: number;
    read<T = TDataIn>(hp?: number): Promise<Result<T, StreamErr>>;
    write<T = TDataOut>(data: T): Promise<Result<void, StreamErr>>;
    waitClose(): Promise<StreamCloseCodes>;
    closeOk(): void;
    closeErr(): void;
}
```

A bidirectional, typed message channel multiplexed over a [`Socket`](#socket). Streams are created by the library ([`outgoingStream()`](#socketoutgoingstream), [`incomingStream()`](#socketincomingstream)); you never construct them.

| Type parameter | Description                                       |
| -------------- | ------------------------------------------------- |
| `TArgs`        | Type of the arguments the stream was opened with. |
| `TDataIn`      | Type of the messages you read.                    |
| `TDataOut`     | Type of the messages you write.                   |

| Member                | Default | Description                                                                                                                                                                      |
| --------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                  |         | UUID v4 chosen by the side that opened the stream.                                                                                                                               |
| `args`                |         | Arguments from `new_stream`: your own object for outgoing streams, the peer's parsed JSON for incoming ones (not validated).                                                     |
| `active`              |         | `true` until the stream is closed by either side, lost, or its connection is destroyed.                                                                                          |
| `closeCode`           |         | The [`StreamCloseCodes`](#streamclosecodes) value once closed, `undefined` while open.                                                                                           |
| `incomingQueueCap`    | `256`   | Maximum unread incoming messages; one more closes the stream with `Error` (buffered messages stay readable). Changing it never drops messages.                                   |
| `outgoingQueueCap`    | `256`   | Maximum queued outgoing messages; one more closes the stream with `Error` and fails all queued writes with `Dead`.                                                               |
| `incomingQueueLength` |         | Messages waiting to be read.                                                                                                                                                     |
| `outgoingQueueLength` |         | Messages waiting to be sent.                                                                                                                                                     |
| `read(hp?)`           | `30`    | Next message; see [below](#streamread).                                                                                                                                          |
| `write(data)`         |         | Send a message; see [below](#streamwrite).                                                                                                                                       |
| `waitClose()`         |         | Resolves with the close code (immediately if already closed). Never rejects.                                                                                                     |
| `closeOk()`           |         | Sends `close_stream` `Normal` and closes: a waiting read gets `Dead`, queued writes are discarded (`Dead`), buffered incoming messages stay readable. No-op when already closed. |
| `closeErr()`          |         | Same as `closeOk()` with the code `Error`.                                                                                                                                       |

```ts
once(async () => {
    const code = await stream.waitClose();
    if (code === StreamCloseCodes.Lost) console.warn(`stream ${stream.id} lost; open a new one`);
});
```

##### `Stream.read()`

```ts
read<T = TDataIn>(hp?: number): Promise<Result<T, StreamErr>>
```

Returns a buffered message immediately, even after the stream closed, so nothing that arrived is lost; otherwise waits for the next message for at most `hp` ticks. Only one read can wait at a time: starting another resolves the earlier one with `StreamErr.Concurrent`.

| Parameter | Type     | Default   | Description                                                                                            |
| --------- | -------- | --------- | ------------------------------------------------------------------------------------------------------ |
| `hp`      | `number` | `30`      | Timeout in ticks (seconds). Use a positive integer; `0`, negative or fractional values never time out. |
| `T`       | type     | `TDataIn` | Expected message type (not validated).                                                                 |

**Returns** `Ok(message)`; `Err(StreamErr.Timeout)` when nothing arrived in time (the stream stays open); `Err(StreamErr.Dead)` when the stream is closed and drained; `Err(StreamErr.Concurrent)` when another read replaced this one. Never rejects.

```ts
const reply = await stream.read<{ status: string }>(10);
if (reply.isOk()) console.log(reply.unwrap().status);
else if (reply.e === StreamErr.Timeout) console.warn('no reply within 10 s');
```

##### `Stream.write()`

```ts
write<T = TDataOut>(data: T): Promise<Result<void, StreamErr>>
```

Sends the message immediately when possible, otherwise queues it (see [Stream backpressure and queue caps](#stream-backpressure-and-queue-caps)).

| Parameter | Type | Description                                                                                                                     |
| --------- | ---- | ------------------------------------------------------------------------------------------------------------------------------- |
| `data`    | `T`  | JSON-serialisable message; `undefined` is allowed. Values `JSON.stringify` rejects are never sent and block the outgoing queue. |

**Returns** `Ok` once the frame was handed to the WebSocket (not when the peer read it), or `Err(StreamErr.Dead)` when the stream is or becomes closed first. Never rejects.

```ts
for (const row of rows) {
    if ((await stream.write(row)).isErr()) break; // the stream closed
}
stream.closeOk();
```

#### `StreamErr`

Source: [src/stream.ts](src/stream.ts)

```ts
enum StreamErr {
    Dead = 'Dead',
    Concurrent = 'Concurrent',
    Timeout = 'Timeout',
}
```

Errors of [`Stream.read()`](#streamread) and [`Stream.write()`](#streamwrite). See [Stream and socket errors](#stream-and-socket-errors).

```ts
const message = await stream.read(5);
if (message.isErr() && message.e === StreamErr.Dead) console.log('stream finished');
```

#### `InitStreamErr`

Source: [src/stream.ts](src/stream.ts)

```ts
enum InitStreamErr {
    Dead = 'Dead',
    Lost = 'Lost',
}
```

Errors of [`Socket.outgoingStream()`](#socketoutgoingstream) and [`Connection.newOutgoingStream()`](#connection). Every failed opening currently reports `Dead`; `Lost` is reserved and not produced. See [Stream and socket errors](#stream-and-socket-errors).

```ts
const opened = await socket.outgoingStream('export');
if (opened.isErr() && opened.e === InitStreamErr.Dead) console.warn('the server did not accept the stream');
```

#### `StreamCloseCodes`

Source: [src/stream.ts](src/stream.ts)

```ts
enum StreamCloseCodes {
    Normal = 'Normal',
    Lost = 'Lost',
    NotFound = 'NotFound',
    Duplicate = 'Duplicate',
    Error = 'Error',
}
```

Codes carried by `close_stream` and reported by [`Stream.closeCode`](#stream) and [`Stream.waitClose()`](#stream). See [Stream close codes](#stream-close-codes). Codes received from the peer are not validated.

```ts
if ((await stream.waitClose()) !== StreamCloseCodes.Normal) console.warn('stream ended abnormally');
```

### Protocol codecs

#### `Message`

Source: [src/messages.ts](src/messages.ts)

```ts
import { Message } from '@araminix/lib';

// Message.NewStream, Message.AcceptEndpoint, Message.AcceptStream, Message.CloseStream,
// Message.DataStream, Message.PingStream, Message.PongStream (io-ts codecs)
// Message.NewStreamType, Message.AcceptEndpointType, ... (their decoded TypeScript types)
```

Namespace with the io-ts codecs of every protocol frame (see [Protocol messages](#protocol-messages)) and their decoded types. The connection validates incoming frames with them; you can use them to inspect frames, write test doubles or implement a compatible peer. Each codec is a `t.type(...)`: `.is(value)` is a type guard and `.decode(value)` returns an io-ts `Either` with detailed errors.

| Codec            | Type alias           | Shape                                                             |
| ---------------- | -------------------- | ----------------------------------------------------------------- |
| `NewStream`      | `NewStreamType`      | `{ type: 'new_stream'; id: string; name: string; args: unknown }` |
| `AcceptEndpoint` | `AcceptEndpointType` | `{ type: 'accept_endpoint' }`                                     |
| `AcceptStream`   | `AcceptStreamType`   | `{ type: 'accept_stream'; id: string }`                           |
| `CloseStream`    | `CloseStreamType`    | `{ type: 'close_stream'; id: string; code: string }`              |
| `DataStream`     | `DataStreamType`     | `{ type: 'data_stream'; id: string; data: unknown }`              |
| `PingStream`     | `PingStreamType`     | `{ type: 'ping_stream'; id: string }`                             |
| `PongStream`     | `PongStreamType`     | `{ type: 'pong_stream'; id: string }`                             |

```ts
import { Message } from '@araminix/lib';

const frame: unknown = JSON.parse('{"type":"close_stream","id":"s-1","code":"Normal"}');
if (Message.CloseStream.is(frame)) {
    console.log(`stream ${frame.id} closed with ${frame.code}`);
}
```

### Results

#### `Result`

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
type Result<T, E> = Ok<T> | Err<E, T>;
```

The outcome of an operation that can fail: [`Ok<T>`](#ok) with a value or [`Err<E>`](#err) with an error. Narrow with `isOk()` / `isErr()`; see [The Result monad](#the-result-monad).

| Type parameter | Description                |
| -------------- | -------------------------- |
| `T`            | Type of the success value. |
| `E`            | Type of the error value.   |

```ts
function divide(a: number, b: number): Result<number, 'DivisionByZero'> {
    return b === 0 ? new Err('DivisionByZero') : new Ok(a / b);
}
```

#### `Ok`

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
declare class Ok<T> {
    readonly v: T;
    constructor(v: T);
    isOk(): this is Ok<T>; // always true
    isErr(): this is Err<never, T>; // always false
    unwrap(): T; // returns v
    serialize(): { ok: true; v: T };
}
```

The success variant of [`Result`](#result). `serialize()` produces the `{ ok: true, v }` wire format used by [`AgentClient`](#agentclient).

```ts
const result = new Ok({ id: 7 });
result.unwrap().id; // 7
JSON.stringify(result.serialize()); // '{"ok":true,"v":{"id":7}}'
```

#### `Err`

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
declare class Err<E = void, T = void> {
    readonly e: E;
    constructor(e: E);
    isOk(): this is Ok<never>; // always false
    isErr(): this is Err<E, never>; // always true
    unwrap(): T; // always throws
    serialize(): { ok: false; e: E };
}
```

The failure variant of [`Result`](#result). `unwrap()` always throws: `e` itself when it is an `Error` instance, otherwise an [`UnwrapError`](#unwraperror) that keeps `e`. `serialize()` produces `{ ok: false, e }`; note that `JSON.stringify` turns an `Error` into `{}`, so send strings or plain objects to a peer.

| Type parameter | Description                                                            |
| -------------- | ---------------------------------------------------------------------- |
| `E`            | Type of the error value (default `void`).                              |
| `T`            | Success type of the `Result` it stands in for (only types `unwrap()`). |

```ts
const failure = new Err('NotFound');
failure.isErr(); // true
JSON.stringify(failure.serialize()); // '{"ok":false,"e":"NotFound"}'
```

#### `UnwrapError`

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
declare class UnwrapError<E = unknown> extends Error {
    readonly e: E; // the original error value, also available as `cause`
    constructor(e: E);
}
```

Thrown by [`Err.unwrap()`](#err) when the error value is not an `Error`. `name` is `'UnwrapError'`, `message` is `unwrap() called on an Err: <value>` (strings as-is, other values as JSON or `String(value)`), and both `e` and the standard `cause` hold the original value. Added in 3.0.0; see [Migration notes](#migration-notes).

```ts
try {
    new Err(StreamErr.Timeout).unwrap();
} catch (error) {
    if (error instanceof UnwrapError && error.e === StreamErr.Timeout) console.warn('timed out');
}
```

#### `ok()` helper

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
declare function ok(): Ok<void>;
```

Returns `new Ok(undefined)`, the success value of an [`R`](#r).

```ts
const done: R = ok();
```

#### `err()` helper

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
declare function err(): Err<void, void>;
```

Returns `new Err(undefined)`, the failure value of an [`R`](#r). Calling `unwrap()` on it throws an [`UnwrapError`](#unwraperror).

```ts
const failed: R = err();
```

#### `R`

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
type R = Result<void, void>;
```

A payload-less [`Result`](#result): "did it work?". Returned by the low-level [`Connection`](#connection) writers, by `Connection.accept()` and by the resolver of [`deal()`](#deal).

```ts
function save(value: string): R {
    return value.length > 0 ? ok() : err();
}
```

#### `deal()`

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
declare function deal<T>(): Deal<T>; // [promise, resolve]
```

Creates a promise together with a resolver you can call from anywhere. The resolver settles the promise once: the first call returns `ok()`, later calls do nothing and return `err()`, which makes it safe to race several completion paths. The promise never rejects; use a [`Result`](#result) as `T` to model failures.

| Type parameter | Description                                  |
| -------------- | -------------------------------------------- |
| `T`            | Type of the value the promise resolves with. |

**Returns** `[promise, resolve]` (see [`Deal`](#deal-type)).

```ts
import { deal, Err, Ok, Result } from '@araminix/lib';
import { EventEmitter } from 'node:events';

const events = new EventEmitter();
const [answer, resolve] = deal<Result<string, 'Timeout'>>();
const timer = setTimeout(() => resolve(new Err('Timeout')), 5000);
events.once('message', (text: string) => resolve(new Ok(text)));

const result = await answer; // whichever came first; the other resolve() returns err()
clearTimeout(timer);
```

#### `Deal` type

Source: [src/utils/result.ts](src/utils/result.ts)

```ts
type Deal<T> = [promise: Promise<T>, resolve: (value: T) => R];
```

The tuple returned by [`deal()`](#deal).

```ts
const [ready, markReady]: Deal<void> = deal<void>();
markReady(); // ok()
markReady(); // err(): already resolved
```

### Utilities

#### `loop()`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
declare function loop(fun: () => Promise<LoopResult>, onError?: (error: unknown) => LoopResult | Promise<LoopResult>): void;
```

Runs `fun` over and over in the background, one iteration at a time, until it returns anything other than [`LoopResult.Cont`](#loopresult). Errors never escape: when `fun` throws or rejects, the error is logged (`'Unhandled loop error:'`) and `onError` decides what happens; without `onError` the loop restarts on the next macrotask.

| Parameter | Type                                                    | Default     | Description                                                                                                                                                 |
| --------- | ------------------------------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fun`     | `() => Promise<LoopResult>`                             | required    | Loop body. Return `LoopResult.Cont` to run again.                                                                                                           |
| `onError` | `(error: unknown) => LoopResult \| Promise<LoopResult>` | `undefined` | Called after an iteration threw. `Cont` restarts the loop (after `setTimeout(0)`); anything else stops it. If it throws, that is logged and the loop stops. |

**Returns** nothing; the loop runs asynchronously. Successful iterations are chained as microtasks, so each iteration must await real I/O or a timer.

```ts
loop(async () => {
    const incoming = await socket.incomingStream('jobs');
    if (incoming.isErr()) return LoopResult.Stop;
    incoming.unwrap().closeOk();
    return LoopResult.Cont;
});
```

#### `LoopResult`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
enum LoopResult {
    Cont = 0,
    Stop = 1,
}
```

Return value of a [`loop()`](#loop) body (and of its `onError`): `Cont` runs it again, `Stop` (or any other value) ends it.

```ts
let remaining = 3;
loop(async () => {
    await sleep(1000);
    console.log('tick', remaining);
    return --remaining > 0 ? LoopResult.Cont : LoopResult.Stop;
});
```

#### `once()`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
declare function once(fun: () => Promise<void> | void): void;
```

Runs `fun` immediately, fire-and-forget, and makes sure neither a synchronous throw nor a rejected promise escapes; both are logged as `'Unhandled once error:'`. Use it to start background work from synchronous code.

| Parameter | Type                          | Description                             |
| --------- | ----------------------------- | --------------------------------------- |
| `fun`     | `() => Promise<void> \| void` | The work to run. Its result is ignored. |

```ts
once(async () => {
    console.log('socket closed:', await socket.waitClose());
});
```

#### `wrap()`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
declare function wrap<OkType, ErrType = Error>(exe: Promise<OkType> | (() => Promise<OkType>)): Promise<Result<OkType, ErrType>>;
```

Awaits a promise (or calls an async function) and returns `Ok(value)` or `Err(error)` instead of rejecting. Synchronous throws from a function are caught too. Only real `Promise` instances are awaited directly; wrap other thenables in a function.

| Parameter / type parameter | Type                                         | Default  | Description                                     |
| -------------------------- | -------------------------------------------- | -------- | ----------------------------------------------- |
| `exe`                      | `Promise<OkType> \| (() => Promise<OkType>)` | required | The work to await.                              |
| `ErrType`                  | type                                         | `Error`  | Type the caught error is cast to (not checked). |

**Returns** a promise of [`Result<OkType, ErrType>`](#result) that never rejects.

```ts
const response = await wrap(fetch('https://api.example.com/health'));
if (response.isOk()) console.log(response.v.status);
```

#### `wrapSync()`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
declare function wrapSync<OkType, ErrType = Error>(exe: () => OkType): Result<OkType, ErrType>;
```

Calls `exe` and returns `Ok(returnValue)` or `Err(thrownValue)`; never throws.

| Parameter / type parameter | Type           | Default  | Description                                     |
| -------------------------- | -------------- | -------- | ----------------------------------------------- |
| `exe`                      | `() => OkType` | required | The function to call.                           |
| `ErrType`                  | type           | `Error`  | Type the caught error is cast to (not checked). |

```ts
const parsed = wrapSync<{ debug: boolean }, SyntaxError>(() => JSON.parse('{"debug":true}'));
if (parsed.isErr()) console.error(parsed.e.message);
```

#### `sleep()`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
declare function sleep(ms: number): Promise<void>;
```

Resolves after `ms` milliseconds.

| Parameter | Type     | Description            |
| --------- | -------- | ---------------------- |
| `ms`      | `number` | Delay in milliseconds. |

```ts
await sleep(250);
```

#### `uuid()`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
declare function uuid(): string;
```

Returns a random RFC 4122 version 4 UUID. Uses `crypto.randomUUID()` when it exists; in browsers that function is only available in secure contexts (HTTPS or localhost), so otherwise the UUID is built from `crypto.getRandomValues()`. Stream ids are generated with it.

```ts
const requestId = uuid(); // e.g. '3b241101-e2bb-4255-8caf-4136c566a962'
```

#### `trimSlash()`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
declare function trimSlash(str: string): string;
```

Removes all leading and trailing `/` characters (inner slashes are kept). Used to normalise [`ClientOptions.mount`](#client-options).

| Parameter | Type     | Description         |
| --------- | -------- | ------------------- |
| `str`     | `string` | The string to trim. |

```ts
trimSlash('//api/v1//'); // 'api/v1'
```

#### `Defined`

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
type Defined<T> = T extends undefined ? never : T;
```

Removes `undefined` from a type (distributing over unions), like `Exclude<T, undefined>`.

```ts
type Port = Defined<number | undefined>; // number
```

#### `t` (io-ts namespace)

Source: [src/utils/index.ts](src/utils/index.ts)

```ts
import * as t from 'io-ts';

// re-exported as `t`
```

The [io-ts](https://github.com/gcanti/io-ts) namespace the protocol codecs are built with, re-exported so you can validate your own stream payloads with the same version.

```ts
import { t } from '@araminix/lib';

const Job = t.type({ id: t.string, priority: t.number });
const message = await stream.read();
if (message.isOk() && Job.is(message.unwrap())) {
    console.log('valid job');
}
```

### Logging

#### `Logger`

Source: [src/utils/logger.ts](src/utils/logger.ts)

```ts
declare class Logger {
    constructor(options?: LoggerConfig);
    configure(options: Partial<LoggerConfig>): void;
    initStore(options: StoreOptions): void;
    setLevel(level: LOG_LEVEL): void;
    error<T extends unknown[]>(...args: T): void;
    warn<T extends unknown[]>(...args: T): void;
    info<T extends unknown[]>(...args: T): void;
    debug<T extends unknown[]>(...args: T): void;
    trace<T extends unknown[]>(...args: T): void;
    static getTimestamp(): string;
    static getTodaysDate(): string;
}
```

Levelled logger for the console, with optional daily log files in Node.js. `WARN` and `ERROR` go to `console.error`, other levels to `console.log`.

| Member                        | Description                                                                                                                                                                     |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `constructor(options?)`       | A complete [`LoggerConfig`](#loggerconfig). Default: level `DEBUG`, stack traces and timestamps on, `'text'` format, no files, no prefix.                                       |
| `configure(options)`          | Merges any subset of `LoggerConfig`. Changing `level` updates the console threshold; passing `storeOptions` (even `null`) re-initialises the directory on the next stored line. |
| `initStore(options)`          | Enables file storage with [`StoreOptions`](#storeoptions) and starts creating the directory right away. Failures are reported with `console.error`, never thrown.               |
| `setLevel(level)`             | Changes the console threshold only; files keep `StoreOptions.level`.                                                                                                            |
| `error/warn/info/debug/trace` | Log at that level. In `'text'` format arguments are passed to the console unchanged; JSON output and files serialise each argument and join them with spaces.                   |
| `Logger.getTimestamp()`       | Current UTC time as `YYYY-MM-DD - HH:mm:ss.sss`.                                                                                                                                |
| `Logger.getTodaysDate()`      | Current UTC date as `YYYY-MM-DD` (used in file names).                                                                                                                          |

```ts
const log = new Logger({ level: LOG_LEVEL.INFO, showStackTrace: false, showTimestamp: true, format: 'json', prefix: 'api' });
log.info('started'); // {"timestamp":"...","level":"INFO","prefix":" [api]","message":"started"}
```

#### `logger` instance

Source: [src/utils/logger.ts](src/utils/logger.ts)

```ts
declare const logger: Logger;
```

The shared default [`Logger`](#logger) (level `DEBUG`, `'text'` format, console only). The library logs through it, so configure it to control the library's output; see [Logging and file storage](#logging-and-file-storage).

```ts
logger.configure({ level: LOG_LEVEL.WARN });
```

#### `LOG_LEVEL`

Source: [src/utils/logger.ts](src/utils/logger.ts)

```ts
enum LOG_LEVEL {
    TRACE = 'trace',
    INFO = 'info',
    DEBUG = 'debug',
    WARN = 'warn',
    ERROR = 'error',
}
```

Log levels. Priority (not declaration order): `TRACE` (0) < `DEBUG` (1) < `INFO` (2) < `WARN` (3) < `ERROR` (4). A message is printed when its priority is at least the threshold's.

```ts
logger.setLevel(LOG_LEVEL.TRACE); // print every incoming WebSocket frame
```

#### `LoggerConfig`

Source: [src/utils/logger.ts](src/utils/logger.ts)

```ts
interface LoggerConfig {
    level: LOG_LEVEL;
    showStackTrace: boolean;
    showTimestamp: boolean;
    format: 'json' | 'text';
    storeOptions?: StoreOptions | null;
    prefix?: string | null;
}
```

Configuration of a [`Logger`](#logger). Fields and defaults are listed under [Logger options](#logger-options).

```ts
const config: LoggerConfig = { level: LOG_LEVEL.INFO, showStackTrace: true, showTimestamp: true, format: 'text', prefix: 'worker' };
```

#### `StoreOptions`

Source: [src/utils/logger.ts](src/utils/logger.ts)

```ts
interface StoreOptions {
    directory: string;
    level: LOG_LEVEL;
    filePrefix?: string;
}
```

File storage settings of a [`Logger`](#logger) (Node.js only). Fields and defaults are listed under [Log storage options](#log-storage-options).

```ts
logger.initStore({ directory: './logs', level: LOG_LEVEL.WARN, filePrefix: 'agent' });
```

## Configuration reference

### Client options

[`ClientOptions`](#clientoptions), passed to `new Client(options)`.

| Option                 | Type     | Default     | Description                                                                                                                                                                                                                                                                       |
| ---------------------- | -------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                 | `string` | required    | Base URL for HTTP calls, without a trailing slash, e.g. `'https://api.example.com'`.                                                                                                                                                                                              |
| `wsHost`               | `string` | required    | Base URL for WebSocket endpoints, without a trailing slash, e.g. `'wss://api.example.com'`. Endpoints live at `${wsHost}${mount}/ws/<name>[/<arg>]`.                                                                                                                              |
| `mount`                | `string` | `''`        | Path prefix for HTTP and WebSocket URLs. Leading and trailing slashes are normalised: `'api/v1/'` becomes `/api/v1`.                                                                                                                                                              |
| `token`                | `string` | `undefined` | Node.js only. Sent as the `x-token` header on the WebSocket upgrade and on every HTTP call. Leave unset in browsers, which authenticate with the session cookie.                                                                                                                  |
| `hp`                   | `number` | `300`       | Connection hp in ticks (seconds): time without an open socket, summed over the waits between reconnect attempts and refilled whenever a socket opens, before the connection is destroyed with `CloseCodes.Died`.                                                                  |
| `connectTimeout`       | `number` | `undefined` | Milliseconds until the endpoint must be accepted, across reconnect attempts. On expiry `connect()` resolves `ConnectFail.Unexpected`. Negative values disable it; `0` practically always fails; no effect once accepted.                                                          |
| `maxReconnectAttempts` | `number` | `undefined` | Maximum consecutive reconnects after transient closes; `0` disables reconnecting, `undefined` means unlimited. Reaching it destroys the connection (`ConnectFail.Unexpected` if `connect()` is still pending). The count resets after 5 s of open socket and on first acceptance. |

### Connection options

[`ConnectionInterface`](#connectioninterface), passed to `new Connection(inter, arg)`. All durations are in ticks of the 1-second connection timer; use positive integers.

| Option              | Type                     | Default     | Description                                                                                                                                              |
| ------------------- | ------------------------ | ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hp`                | `number`                 | `300`       | Ticks without an attached WebSocket before the connection is destroyed with `CloseCodes.Died`. Refilled when a socket opens or is attached with `reset`. |
| `streamRequestHp`   | `number`                 | `10`        | Ticks a received `new_stream` waits for `acquireStream()` before it is dropped and the peer gets `close_stream` `Lost`.                                  |
| `maxPendingStreams` | `number`                 | `undefined` | Maximum queued (unacquired) incoming stream requests across all names; further requests get `close_stream` `Error`. `undefined` means no limit.          |
| `onDestroy`         | `() => void`             | required    | Called once at the end of `destroy()`.                                                                                                                   |
| `onDisconnect`      | `(code: number) => void` | required    | Called when the attached WebSocket closes, with its close code. The connection stays alive; the owner decides what to do.                                |
| `onAccept`          | `() => void`             | required    | Called when the first `accept_endpoint` arrives (client side); later ones are ignored.                                                                   |
| `onOpen`            | `() => void`             | `undefined` | Called whenever the attached WebSocket fires `open`.                                                                                                     |

### Logger options

[`LoggerConfig`](#loggerconfig), passed to `new Logger(config)` (complete) or `logger.configure(options)` (any subset).

| Option           | Type                   | Default           | Description                                                                                                                                                                                         |
| ---------------- | ---------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `level`          | `LOG_LEVEL`            | `LOG_LEVEL.DEBUG` | Console threshold. File storage has its own threshold.                                                                                                                                              |
| `showStackTrace` | `boolean`              | `true`            | Include stacks when errors are serialised (JSON output and files); otherwise `Name: message`. In `'text'` output arguments are passed to the console unchanged.                                     |
| `showTimestamp`  | `boolean`              | `true`            | Prefix console output with the UTC timestamp. File lines always include it.                                                                                                                         |
| `format`         | `'json' \| 'text'`     | `'text'`          | `'text'`: coloured `[timestamp] [LEVEL] [prefix]` header plus the raw arguments. `'json'`: one JSON string with `timestamp`, `level`, `prefix` and `message` per call. Files always use text lines. |
| `storeOptions`   | `StoreOptions \| null` | `null`            | File storage settings; `null` or `undefined` for console only. See also `initStore()`.                                                                                                              |
| `prefix`         | `string \| null`       | `null`            | Label printed as ` [prefix]` after the level in console output (not in files).                                                                                                                      |

### Log storage options

[`StoreOptions`](#storeoptions), passed to `logger.initStore(options)` or as `LoggerConfig.storeOptions`. Node.js only.

| Option       | Type        | Default     | Description                                                                                                                                                  |
| ------------ | ----------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `directory`  | `string`    | required    | Directory of the log files, resolved against `process.cwd()` and created recursively if missing.                                                             |
| `level`      | `LOG_LEVEL` | required    | Minimum level written to files, independent of the console threshold.                                                                                        |
| `filePrefix` | `string`    | `'folkrum'` | File name prefix: `<filePrefix>-logs-<YYYY-MM-DD>.log` for every stored level except `ERROR`, `<filePrefix>-errors-<YYYY-MM-DD>.log` for `ERROR` (UTC date). |

## Close codes and error enums

### WebSocket close codes

[`WsCloseCodes`](#wsclosecodes) and how [`Client.connect()`](#clientconnect) reacts to them, before and after the endpoint was accepted.

| Code          | Member                 | Meaning                                                             | `connect()` result (pending) | Connection closed with    |
| ------------- | ---------------------- | ------------------------------------------------------------------- | ---------------------------- | ------------------------- |
| 4000          | `Normal`               | Deliberate close; sent whenever a connection is destroyed.          | `ConnectFail.Unexpected`     | `CloseCodes.Intentional`  |
| 4401          | `Unauthorized`         | Not authenticated (missing or expired cookie, invalid token).       | `ConnectFail.Unauthorized`   | `CloseCodes.Unauthorized` |
| 4403          | `Denied`               | Endpoint or origin refused (`Connection.deny()`).                   | `ConnectFail.Denied`         | `CloseCodes.Unauthorized` |
| 4404          | `NotFound`             | Unknown endpoint.                                                   | `ConnectFail.NotFound`       | `CloseCodes.Unexpected`   |
| 4409          | `ConcurrentConnection` | Another connection for the same client, endpoint and arg took over. | `ConnectFail.Concurrent`     | `CloseCodes.Concurrent`   |
| 1011          | `InternalError`        | Server error (RFC 6455). Transient.                                 | reconnect with backoff       | (stays open)              |
| anything else | none                   | Transient (e.g. 1006 abnormal closure, 1008).                       | reconnect with backoff       | (stays open)              |

### Connection close reasons

[`CloseCodes`](#closecodes), reported by [`Socket.waitClose()`](#socket), [`AgentClient.waitClose()`](#agentclient) and `Connection.closePromise`. The causes listed are the ones the client produces; servers pick their own codes when they call `destroy()`.

| Value            | Cause on the client                                                                                            |
| ---------------- | -------------------------------------------------------------------------------------------------------------- |
| `'Died'`         | No WebSocket attached for `hp` ticks.                                                                          |
| `'Unexpected'`   | Connect timeout, reconnect limit, close code 4404, no global `WebSocket`, or `accept()` could not be sent.     |
| `'Intentional'`  | `Socket.destroy()` / `AgentClient.destroy()`, or the server closed with 4000 (e.g. after destroying its side). |
| `'Concurrent'`   | Close code 4409: taken over by another connection.                                                             |
| `'Unauthorized'` | Close code 4401 or 4403, or `Connection.deny()`.                                                               |

### Connect failures

[`ConnectFail`](#connectfail), the error of [`Client.connect()`](#clientconnect) and `connectAgent()`.

| Value            | Cause                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `'Unauthorized'` | Close code 4401.                                                                                                            |
| `'Denied'`       | Close code 4403.                                                                                                            |
| `'NotFound'`     | Close code 4404.                                                                                                            |
| `'Concurrent'`   | Close code 4409 (since 2.1.0; earlier versions reported `Unexpected`).                                                      |
| `'Unexpected'`   | Everything else: close code 4000, `connectTimeout`, `maxReconnectAttempts`, connection hp exhausted, no global `WebSocket`. |

### Stream close codes

[`StreamCloseCodes`](#streamclosecodes), carried by `close_stream`.

| Value         | Cause                                                                                                                 |
| ------------- | --------------------------------------------------------------------------------------------------------------------- |
| `'Normal'`    | `closeOk()`.                                                                                                          |
| `'Error'`     | `closeErr()`, an incoming or outgoing queue overflow, or the receiver's `maxPendingStreams` was reached.              |
| `'Lost'`      | Heartbeat expired, the connection was destroyed, or a queued request was not acquired within `streamRequestHp` ticks. |
| `'NotFound'`  | A frame referred to a stream id the receiver does not know (for example one it already closed).                       |
| `'Duplicate'` | A `new_stream` reused an id that is already live or queued.                                                           |

### Stream and socket errors

| Enum                              | Value              | Returned by                                       | Meaning                                                                                              |
| --------------------------------- | ------------------ | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| [`StreamErr`](#streamerr)         | `'Timeout'`        | `read(hp)`                                        | Nothing arrived within `hp` ticks; the stream is still open.                                         |
| [`StreamErr`](#streamerr)         | `'Dead'`           | `read()`, `write()`                               | The stream is closed (for `read()`: and drained); for `write()`: closed before the message was sent. |
| [`StreamErr`](#streamerr)         | `'Concurrent'`     | `read()`                                          | Another `read()` replaced this one.                                                                  |
| [`InitStreamErr`](#initstreamerr) | `'Dead'`           | `outgoingStream()`, `newOutgoingStream()`         | Connection dead, stream rejected or closed by the peer, or not accepted within 20 ticks.             |
| [`InitStreamErr`](#initstreamerr) | `'Lost'`           | none                                              | Reserved; not produced by the current implementation.                                                |
| [`SocketErr`](#socketerr)         | `0` (`Closed`)     | `incomingStream()`, `acquireStream()`, `onTask()` | The connection is or became dead.                                                                    |
| [`SocketErr`](#socketerr)         | `1` (`Concurrent`) | `incomingStream()`, `acquireStream()`, `onTask()` | A newer call for the same name replaced this one.                                                    |

### Agent errors

Errors of [`AgentClient.post()`](#agentclientpost), discriminated by [`AgentClientErrorType`](#agentclienterrortype).

| Class                                                             | `type`            | `e` holds                                                                                                         |
| ----------------------------------------------------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------- |
| [`CommunicationClientAgentError`](#communicationclientagenterror) | `'Communication'` | `InitStreamErr` (stream not opened), `StreamErr` (`Timeout`, `Dead`) or `'Format'` (reply without an `ok` field). |
| [`ExecutionClientAgentError`](#executionclientagenterror)         | `'Execution'`     | The `e` of the server's `{ ok: false, e }` reply.                                                                 |

## Migration notes

### Upgrading to 3.0.0

**`Err.unwrap()` always throws an `Error`.** It used to throw the stored value as-is, so string codes and `undefined` were thrown without a stack trace. Now `Error` values are rethrown unchanged and every other value is wrapped in the new [`UnwrapError`](#unwraperror), which keeps the original value in `e` and in `cause`.

```ts
// Before (2.x)
try {
    result.unwrap();
} catch (code) {
    if (code === ConnectFail.Unauthorized) showLogin();
}

// After (3.x)
try {
    result.unwrap();
} catch (error) {
    if (error instanceof UnwrapError && error.e === ConnectFail.Unauthorized) showLogin();
}
```

If you are upgrading from 2.0.x, also apply the 2.1.0 note below.

### Upgrading to 2.1.0

**`ConnectFail.Concurrent` for close code 4409.** When another connection for the same client, endpoint and arg takes over, `connect()` now resolves with the new [`ConnectFail.Concurrent`](#connectfail) instead of `ConnectFail.Unexpected`, so it can be told apart from real failures. Code that treated `Unexpected` as covering this case should also handle `Concurrent`, and exhaustive `switch` statements or `Record<ConnectFail, ...>` maps need the new member.

### Upgrading to 2.0.0

**Native WebSocket, Node.js 22 or later.** The client now opens sockets with the global `WebSocket` of browsers and Node.js, and the `websocket` package (with its native add-ons `bufferutil` and `utf-8-validate`) is no longer a dependency.

- Run on Node.js 22 or later (`engines.node: ">=22"`). Without a global `WebSocket`, `connect()` resolves `Err(ConnectFail.Unexpected)` and logs the reason.
- In Node.js the token is still sent as the `x-token` header, now through the built-in `new WebSocket(url, { headers })`; browsers still rely on the session cookie.
- Installs no longer pull in `websocket`, its native add-ons or `es5-ext`, which also simplifies worker-thread and bundler setups.

## Testing

The tests use Node's built-in test runner and run against the compiled output, so build first. They replace the global `WebSocket` and axios with in-memory fakes, so no server is needed.

```bash
pnpm install
pnpm build          # tsc: compiles src/, index.ts and tests/ to dist/
pnpm test           # node --test "dist/tests/**/*.test.js"
pnpm test:coverage  # the same with --experimental-test-coverage
```

## Related packages

- [**@araminix/platform**](https://github.com/araminix/platform): the server side of the protocol. It provides the Express-based HTTP and WebSocket endpoint (`/auth`, `/2fa/*`, `/file`, `/attachments/*`, `/ws/<name>/<arg>`), session and token authentication, the agent runtime that answers [`AgentClient.post()`](#agentclientpost) and sends [`onTask()`](#agentclientontask) tasks, and uses this package's [`Connection`](#connection) and [`Socket`](#socket) for every endpoint client.
- Source of this package: [github.com/araminix/lib](https://github.com/araminix/lib).

## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

Copyright (c) 2026 Amin Abbasi ([araminix](https://github.com/araminix)).
