# Trace: Secure Session Token and Headers in Client

- **Package:** `@araminix/lib`
- **Version Bump:** `1.1.0`
- **Audit Findings Fixed:** Finding #24 (Session token exposed to JavaScript, URLs and logs).

## Changes Made
1. `src/client.ts`:
   - Changed `this.token` from storing URL prefix (`ws-token/${token}` / `ws`) to raw token: `private readonly token?: string;`.
   - Updated `connect(name, arg)`: constructs clean URL `${this.wsHost}${this.mount}/ws/${name}${pathArg}` without token in path.
   - Updated `reconnect(url, connection)`: passes `headers: { 'x-token': this.token }` to `w3cwebsocket` in Node when token is present, and single-argument `new w3cwebsocket(url)` when token is omitted (ensuring full browser compatibility).
   - Updated `upload`, `auth`, `gauth`, `deauth`, `request2FA`, `enable2FA`, `verify2FA`, `disable2FA`, `getPresignedUploadUrl`, and `getPresignedDownloadUrl`: added `withCredentials: true` and attached `x-token: this.token` header when token is defined.
2. `tests/client.test.ts`: Added 3 unit tests verifying WebSocket URL construction, header transmission in agent mode, and omission in browser mode.
3. `package.json`: Registered `dist/tests/client.test.js` in `test` and `test:coverage` scripts.
4. `README.md`: Documented browser cookie vs Node agent header authentication patterns and marked `/ws-token/` routes deprecated.
