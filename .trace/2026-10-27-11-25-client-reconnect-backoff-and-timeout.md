# Trace: Client Connection Timeout, Exponential Backoff with Jitter, and Reconnect Limits

- **Package:** `@araminix/lib`
- **Audit Finding Fixed:** "Client.connect() can hang; reconnect storms — Medium"

## Root Cause Analysis

1. `onDestroy` was an empty no-op function in `inter`. If the remote endpoint was unreachable or connection died before acceptance, the `connect()` deferred promise never settled, hanging callers indefinitely.
2. Reconnections were retried every 500 ms constantly with no backoff or jitter (`setTimeout(() => this.reconnect(...), 500)`). If the server restarted, all clients reconnected simultaneously causing thundering herd / reconnect storms. Failed HTTP upgrades (e.g. 401, 404, wrong route, unreachable host) were retried indefinitely with no backoff or ceiling.

## Changes Made

1. `src/connection.ts`:
    - Added optional `onOpen?: () => void` to `ConnectionInterface`.
    - Invoked `this.inter.onOpen?.()` inside `ws.onopen` upon successful underlying WebSocket open.
2. `src/client.ts`:
    - Updated `ClientOptions` to include optional `connectTimeout?: number` and `maxReconnectAttempts?: number`.
    - Updated `Client` constructor and fields to store `connectTimeout` and `maxReconnectAttempts`.
    - Updated `inter.onDestroy` to resolve pending `deal` promise with `new Err(ConnectFail.Unexpected)` and clear active timers. (Second resolutions after `onAccept` are safely ignored by `deal`).
    - Implemented exponential backoff with jitter on reconnects: `min(30s, 500ms * 2^n) * (0.5..1.5)` using `Math.round(Math.min(30000, 500 * Math.pow(2, Math.min(reconnectAttempts, 20))) * (0.5 + Math.random()))`.
    - Added stateful reset of `reconnectAttempts = 0` on `onOpen` and `onAccept`.
    - Handled `connectTimeout` via a timer that resolves with `ConnectFail.Unexpected` and destroys the connection when expired.
    - Handled `maxReconnectAttempts`: when reconnect attempts reach or exceed the limit, terminates retry loop and destroys connection with `ConnectFail.Unexpected`.
    - Added cleanup in `clearTimers()` to avoid orphaned timers and socket leaks.
    - Guarded `reconnect(url, connection)` against executing when `connection.dead` is true.
3. `tests/client.test.ts`:
    - Added unit test suite `Client Connection Lifecycle, Timeouts, and Backoff` covering:
        - `onDestroy` resolution with `ConnectFail.Unexpected`.
        - `connectTimeout` aborting connection and resolving with `ConnectFail.Unexpected`.
        - `connectTimeout` cancellation upon successful `onAccept`.
        - Exponential backoff calculation matching `min(30s, 500ms * 2^n) * (0.5..1.5)` across all attempt ranges.
        - Exponential backoff reset on `ws.onopen`.
        - Exponential backoff reset on `onAccept`.
        - `maxReconnectAttempts` enforcement terminating retries and destroying connection.
        - `maxReconnectAttempts: 0` stopping immediately on first disconnect.
        - `connectAgent` error and timeout propagation.
4. `README.md`:
    - Documented resilient connection lifecycle, exponential backoff with jitter, `connectTimeout`, and `maxReconnectAttempts`.
