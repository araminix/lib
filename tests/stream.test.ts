import assert from 'node:assert';
import { describe, it } from 'node:test';

import { Stream, StreamCloseCodes, StreamController, StreamErr } from '../src/stream.js';
import { err, ok, Ok } from '../src/utils/result.js';

class MockConnection {
    sentData: any[] = [];
    closedStreams: any[] = [];
    writeDataFail: boolean = false;

    writeData(id: string, data: any) {
        if (this.writeDataFail) {
            return err();
        }
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

    it('buffers multiple incoming messages in FIFO order without overwriting', async () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        ctrl.onData('msg-1');
        ctrl.onData('msg-2');
        ctrl.onData('msg-3');

        assert.strictEqual(stream.incomingQueueLength, 3);

        const r1 = await stream.read<string>();
        const r2 = await stream.read<string>();
        const r3 = await stream.read<string>();

        assert.strictEqual(r1.isOk(), true);
        assert.strictEqual(r1.unwrap(), 'msg-1');
        assert.strictEqual(r2.isOk(), true);
        assert.strictEqual(r2.unwrap(), 'msg-2');
        assert.strictEqual(r3.isOk(), true);
        assert.strictEqual(r3.unwrap(), 'msg-3');
        assert.strictEqual(stream.incomingQueueLength, 0);
    });

    it('correctly buffers and delivers undefined payloads', async () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        ctrl.onData(undefined);
        assert.strictEqual(stream.incomingQueueLength, 1);

        const res = await stream.read();
        assert.strictEqual(res.isOk(), true);
        assert.strictEqual(res.unwrap(), undefined);
        assert.strictEqual(stream.incomingQueueLength, 0);
    });

    it('drains buffered items before checking dead when stream is closed', async () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        ctrl.onData('first');
        ctrl.onData('second');
        ctrl.onClose(StreamCloseCodes.Normal);

        assert.strictEqual(stream.active, false);
        assert.strictEqual(stream.closeCode, StreamCloseCodes.Normal);

        const r1 = await stream.read<string>();
        assert.strictEqual(r1.isOk(), true);
        assert.strictEqual(r1.unwrap(), 'first');

        const r2 = await stream.read<string>();
        assert.strictEqual(r2.isOk(), true);
        assert.strictEqual(r2.unwrap(), 'second');

        const r3 = await stream.read<string>();
        assert.strictEqual(r3.isErr(), true);
        if (r3.isErr()) {
            assert.strictEqual(r3.e, StreamErr.Dead);
        }
    });

    it('closes stream with Error code when incomingQueue overflows', async () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;
        stream.incomingQueueCap = 3;

        ctrl.onData('item-1');
        ctrl.onData('item-2');
        ctrl.onData('item-3');
        assert.strictEqual(stream.active, true);

        // 4th item overflows cap of 3
        ctrl.onData('item-4');
        assert.strictEqual(stream.active, false);
        assert.strictEqual(stream.closeCode, StreamCloseCodes.Error);
        assert.strictEqual(conn.closedStreams.length, 1);
        assert.strictEqual(conn.closedStreams[0].code, StreamCloseCodes.Error);

        // Buffered items prior to overflow can still be read
        const r1 = await stream.read<string>();
        assert.strictEqual(r1.unwrap(), 'item-1');
        const r2 = await stream.read<string>();
        assert.strictEqual(r2.unwrap(), 'item-2');
        const r3 = await stream.read<string>();
        assert.strictEqual(r3.unwrap(), 'item-3');

        // Subsequent read returns Dead
        const r4 = await stream.read<string>();
        assert.strictEqual(r4.isErr(), true);
        if (r4.isErr()) {
            assert.strictEqual(r4.e, StreamErr.Dead);
        }
    });

    it('queues pending writes when disconnected and flushes in FIFO order without Concurrent rejection', async () => {
        const conn = new MockConnection();
        conn.writeDataFail = true;
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        const p1 = stream.write('pending-1');
        const p2 = stream.write('pending-2');
        const p3 = stream.write('pending-3');

        assert.strictEqual(stream.outgoingQueueLength, 3);
        assert.strictEqual(conn.sentData.length, 0);

        // Reconnect socket and flush
        conn.writeDataFail = false;
        ctrl._tic();

        const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
        assert.strictEqual(r1.isOk(), true);
        assert.strictEqual(r2.isOk(), true);
        assert.strictEqual(r3.isOk(), true);
        assert.strictEqual(stream.outgoingQueueLength, 0);
        assert.strictEqual(conn.sentData.length, 3);
        assert.deepStrictEqual(conn.sentData, [
            { id: 'test-stream-id', data: 'pending-1' },
            { id: 'test-stream-id', data: 'pending-2' },
            { id: 'test-stream-id', data: 'pending-3' },
        ]);
    });

    it('closes stream with Error and rejects pending writes when outgoingQueue overflows', async () => {
        const conn = new MockConnection();
        conn.writeDataFail = true;
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;
        stream.outgoingQueueCap = 2;

        const p1 = stream.write('w-1');
        const p2 = stream.write('w-2');
        assert.strictEqual(stream.outgoingQueueLength, 2);

        // 3rd write exceeds capacity
        const p3 = stream.write('w-3');

        const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
        assert.strictEqual(r1.isErr(), true);
        if (r1.isErr()) {
            assert.strictEqual(r1.e, StreamErr.Dead);
        }
        assert.strictEqual(r2.isErr(), true);
        if (r2.isErr()) {
            assert.strictEqual(r2.e, StreamErr.Dead);
        }
        assert.strictEqual(r3.isErr(), true);
        if (r3.isErr()) {
            assert.strictEqual(r3.e, StreamErr.Dead);
        }

        assert.strictEqual(stream.active, false);
        assert.strictEqual(stream.closeCode, StreamCloseCodes.Error);
        assert.strictEqual(conn.closedStreams[0].code, StreamCloseCodes.Error);
    });

    it('allows configuring queue capacities', () => {
        const conn = new MockConnection();
        const ctrl = new TestController(conn as any);
        const stream = ctrl.stream;

        assert.strictEqual(stream.incomingQueueCap, 256);
        assert.strictEqual(stream.outgoingQueueCap, 256);

        stream.incomingQueueCap = 512;
        stream.outgoingQueueCap = 128;

        assert.strictEqual(stream.incomingQueueCap, 512);
        assert.strictEqual(stream.outgoingQueueCap, 128);
        assert.strictEqual(ctrl.incomingQueueCap, 512);
        assert.strictEqual(ctrl.outgoingQueueCap, 128);
    });
});
