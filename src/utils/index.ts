import * as t from 'io-ts';

import { deal, Err, err, Ok, ok, R, Result } from './result.js';

export * from './helpers.js';
export * from './logger/index.js';
export * from './result.js';

// Explicit re-exports: `t` is the io-ts namespace (the runtime type codecs used by the protocol messages), so applications can
// validate their own stream payloads with the same io-ts version; the Result helpers are repeated here for convenience.
export { t, Result, Ok, Err, ok, err, R, deal };
