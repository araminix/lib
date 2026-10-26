# @araminix/lib

> **Universal, type-safe multiplexed WebSocket streaming and functional communication protocol.**

[![npm version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://www.npmjs.com/package/@araminix/lib)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.4-blue.svg)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-%3E%3D20.0.0-green.svg)](https://nodejs.org/)

**Araminix Lib** (`@araminix/lib`) provides the foundational communication layer for modern real-time applications. Designed to be completely **isomorphic**, it runs seamlessly across **Node.js**, **Web Browsers**, **Web Workers**, and modern frontend frameworks (React, Vue, Svelte, Angular).

Instead of maintaining multiple raw WebSocket connections or writing complex manual event dispatchers, `@araminix/lib` introduces **typed, multiplexed bidirectional streams** over a single persistent WebSocket connection, backed by a robust **`Result<T, E>` monad** for compile-time error resilience.

### 4. Client Authentication: Browser vs Node Agent

`Client` automatically supports secure session handling:

- **Browser Clients (Cookie Auth):**
  Do not pass `token` to `new Client({ host, wsHost })`. The browser automatically sends the `httpOnly` session cookie with WebSocket upgrades to `/ws/:endpoint` and with `axios` HTTP requests (`withCredentials: true`).

- **Node Clients / Agents (Header Auth):**
  Pass `token: 'my-agent-token'` to `new Client({ host, wsHost, token })`. The client sends `x-token: <token>` in the HTTP upgrade request headers to `/ws/:endpoint` without embedding secrets in the URL.

> [!NOTE]
> The `/ws-token/<token>/...` path-based WebSocket routes are **deprecated** and maintained on the server solely for backward compatibility with legacy clients.

---

## 🌟 Key Features

- ⚡ **Multiplexed Bidirectional Streams**: Run hundreds of independent data streams over a single physical WebSocket connection with isolated lifecycles, flow control, and arguments.
- 🛡️ **Zero-Exception Result Monad**: Functional `Result<T, E>`, `Ok`, `Err`, `deal()`, and `wrap()` abstractions prevent unhandled promise rejections and force explicit error handling.
- 🌐 **Truly Isomorphic / Universal**: Single codebase operating identically in the browser (native `WebSocket`) and Node.js (`ws` / `websocket`).
- 📦 **Typed Protocol Envelopes**: Built-in runtime-validated message types (`NewStream`, `AcceptStream`, `DataStream`, `CloseStream`, `PingStream`, `PongStream`) validated via `io-ts`.
- 🔍 **Structured Logger**: Configurable, timestamped, colored logging utility supporting dynamic levels (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`).
- 🚀 **Zero Overhead**: Minimal dependencies, pure ESM, fully tree-shakeable, and bundled with complete TypeScript `.d.ts` declaration maps.

---

## 📦 Installation

```bash
# npm
npm install @araminix/lib

# pnpm
pnpm add @araminix/lib

# yarn
yarn add @araminix/lib
```

---

## 🚀 Quick Start

### 1. Connecting and Opening a Multiplexed Stream

```typescript
import { Client, Socket, Stream } from '@araminix/lib';

// Connect client to a WebSocket endpoint
const client = new Client({
    url: 'wss://api.example.com/ws/session',
});

const connectResult = await client.connect();

if (connectResult.isOk()) {
    const socket: Socket = connectResult.unwrap();

    // Open an outgoing typed stream over the single WebSocket connection
    const streamResult = await socket.outgoingStream<{ channelId: string }, string, string>('chatRoom', { channelId: 'room-42' });

    if (streamResult.isOk()) {
        const stream = streamResult.unwrap();

        // Send data through the stream
        await stream.write('Hello from client!');

        // Read incoming messages
        const incoming = await stream.read();
        if (incoming.isOk()) {
            console.log('Received:', incoming.unwrap());
        }

        // Gracefully close stream
        stream.closeOk();
    }
}
```

---

### 2. Handling Incoming Streams

```typescript
import { loop, LoopResult, Socket } from '@araminix/lib';

export function handleSocket(socket: Socket) {
    loop(async () => {
        // Wait for an incoming stream named 'sensorData'
        const streamResult = await socket.incomingStream('sensorData');
        if (streamResult.isErr()) return LoopResult.Stop;

        const stream = streamResult.unwrap();
        console.log(`Stream initialized with arguments:`, stream.args);

        // Process stream data
        const readResult = await stream.read();
        if (readResult.isOk()) {
            console.log('Sensor payload:', readResult.unwrap());
            await stream.write({ status: 'ACK' });
        }

        stream.closeOk();
        return LoopResult.Cont;
    });
}
```

---

### 3. Safe Functional Programming with `Result<T, E>`

Eliminate `try/catch` boilerplate and unhandled promise crashes:

```typescript
import { deal, Err, Ok, Result, wrap } from '@araminix/lib';

// 1. Creating typed Results
function parseNumber(input: string): Result<number, string> {
    const val = Number(input);
    return isNaN(val) ? new Err('Invalid number') : new Ok(val);
}

// 2. Safely wrapping third-party promises
const fetchResult = await wrap(fetch('https://api.example.com/data'));
if (fetchResult.isOk()) {
    console.log('HTTP Status:', fetchResult.unwrap().status);
} else {
    console.error('Network failure:', fetchResult.e);
}

// 3. Deferred execution with deal()
const [promise, resolve] = deal<string>();
setTimeout(() => resolve(new Ok('Completed!')), 100);
const result = await promise;
```

---

## 📐 Architecture Overview

```
                   Single WebSocket Connection
Client (Browser / Node) <=====================> Server (@araminix/platform)
                             |          |
            +----------------+          +----------------+
            |                                            |
     Stream "chat"                               Stream "telemetry"
   [DataStream -> "hello"]                    [DataStream -> { cpu: 45 }]
```

---

## 🛠️ API Reference

### Core Classes & Exports

| Export                     | Type     | Description                                                                              |
| -------------------------- | -------- | ---------------------------------------------------------------------------------------- |
| `Socket`                   | Class    | High-level wrapper over a connection providing stream multiplexing and state inspection. |
| `Connection`               | Class    | Manages low-level WebSocket frames, keepalives, and stream controllers.                  |
| `Stream<TArgs, TIn, TOut>` | Class    | Represents an independent, bidirectional typed channel over a `Socket`.                  |
| `Client`                   | Class    | Isomorphic WebSocket connector supporting headers, tokens, and reconnects.               |
| `Result<T, E>`             | Class    | Monadic result container (`Ok<T>` or `Err<E>`).                                          |
| `Logger`                   | Class    | Leveled structured logger (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`).                   |
| `wrap` / `wrapSync`        | Function | Converts throwing functions/promises into `Result` instances.                            |
| `deal`                     | Function | Creates an inverted deferred promise tuple `[promise, resolve]`.                         |

---

## 🧪 Testing

```bash
# Run unit tests
npm test

# Run tests with built-in code coverage report
npm run test:coverage
```

---

## 👤 Author

**Amin Abbasi**

- Organization: [araminix](https://github.com/araminix)
- GitHub: [@abbasi](https://github.com)

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.
