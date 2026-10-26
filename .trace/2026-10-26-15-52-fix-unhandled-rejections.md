# Trace: Fix Unhandled Rejections in loop() and once()

- **Package:** `@araminix/lib`
- **Version Bump:** `1.0.0` -> `1.1.0`
- **Audit Findings Fixed:** Finding #1 (`loop()` and `once()` unhandled rejections).

## Changes Made
1. `src/connection.ts`: Added `InternalError = 1011` to `WsCloseCodes` (RFC 6455 standard close code).
2. `src/utils/index.ts`:
   - Updated `loop(fun, onError?)`: Wrapped `fun()` in `Promise.resolve().then(fun)`. Added rejection handler calling `logger.error` and continuing on the next macrotask via `setTimeout(..., 0)` by default. If `onError` is provided, awaits `onError(error)` and only continues if it returns `LoopResult.Cont`. Wrapped `onError` call in `try/catch` to suppress secondary exceptions.
   - Updated `once(fun)`: Wrapped synchronous invocation in `try/catch` and attached a `.catch()` rejection handler to any returned Promise/thenable. Logged all errors with `logger.error`.
3. `tests/utils.test.ts`: Added 4 unit tests covering async rejection recovery, `onError` returning `Stop`, synchronous throw handling, and `once` error suppression.
