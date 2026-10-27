import axios from 'axios';
import assert from 'node:assert';
import { describe, it } from 'node:test';

import { Client } from '../src/client.js';

describe('Client attachment and file APIs', () => {
    const client = new Client({ host: 'http://127.0.0.1:3000', wsHost: 'ws://127.0.0.1:3000', mount: '/api' });

    it('requests presigned upload URLs from POST /attachments/upload with mimetype and fileName', async () => {
        const originalPost = axios.post;
        let call: any;
        (axios as any).post = async (url: string, data: any, config: any) => {
            call = { url, data, config };
            return { data: { url: 'https://minio/put', key: 'u1/abc.pdf' } };
        };
        try {
            const res = await client.getPresignedUploadUrl('application/pdf', 'invoice.pdf');
            assert.ok(res.isOk());
            assert.strictEqual(call.url, 'http://127.0.0.1:3000/api/attachments/upload');
            assert.deepStrictEqual(call.data, { mimetype: 'application/pdf', fileName: 'invoice.pdf' });
            assert.strictEqual(call.config.withCredentials, true);
        } finally {
            axios.post = originalPost;
        }
    });

    it('requests presigned download URLs from GET /attachments/:key with the key encoded', async () => {
        const originalGet = axios.get;
        let calledUrl = '';
        (axios as any).get = async (url: string) => {
            calledUrl = url;
            return { data: { fileURL: 'https://minio/get' } };
        };
        try {
            const res = await client.getPresignedDownloadUrl('u1/abc.pdf');
            assert.ok(res.isOk());
            assert.strictEqual(calledUrl, 'http://127.0.0.1:3000/api/attachments/u1%2Fabc.pdf');
        } finally {
            axios.get = originalGet;
        }
    });

    it('encodes the file name in file links', () => {
        const link = client.getFileLink({ id: 'f-1', name: 'my report #2?.pdf', size: 1, mimetype: 'application/pdf', userId: 'u1' });
        assert.strictEqual(link, 'http://127.0.0.1:3000/api/file/f-1/my%20report%20%232%3F.pdf');
        assert.strictEqual(client.getFileLink('f-1'), 'http://127.0.0.1:3000/api/file/f-1');
    });
});
