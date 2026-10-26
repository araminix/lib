import assert from 'node:assert';
import { describe, it } from 'node:test';

import { Stream, StreamCloseCodes, StreamController, StreamErr } from '../src/stream.js';
import { ok, Ok } from '../src/utils/result.js';

class MockConnection {
    sentData: any[] = [];
    closedStreams: any[] = [];

    writeData(id: string, data: any) {
        this.sentData.push({ id, data });
        return ok();
    }

    writeClose(id: string, code: StreamCloseCodes) {
        this.closedStreams.push({ id, code });
        return ok();
    }

    writePong() {
        return ok();
    }
    writePing() {
        return ok();
    }
    removeStream() {}
}

class TestController extends StreamController<string, string, string> {
    protected hp = 10;
    protected weak = false;
    get id() {
        return 'test-stream-id';
    }
    get args() {
        return 'test-args';
    }
}

describe('Stream & StreamController', () => {
    it('initializes and reads args', () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        assert.strictEqual(stream.id, 'test-stream-id');
        assert.strictEqual(stream.args, 'test-args');
        assert.strictEqual(stream.active, true);
    });

    it('writes data through stream', async () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        const res = await stream.write('payload-1');
        assert.strictEqual(res.isOk(), true);
        assert.strictEqual(conn.sentData.length, 1);
        assert.deepStrictEqual(conn.sentData[0], { id: 'test-stream-id', data: 'payload-1' });
    });

    it('receives and reads incoming data', async () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        ctrl.onData('incoming-hello');
        const readResult = await stream.read<string>();
        assert.strictEqual(readResult.isOk(), true);
        assert.strictEqual(readResult.unwrap(), 'incoming-hello');
    });

    it('closes gracefully with Normal code', async () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        stream.closeOk();
        assert.strictEqual(stream.active, false);
        const closeCode = await stream.waitClose();
        assert.strictEqual(closeCode, StreamCloseCodes.Normal);
        assert.strictEqual(conn.closedStreams[0].code, StreamCloseCodes.Normal);
    });
});
