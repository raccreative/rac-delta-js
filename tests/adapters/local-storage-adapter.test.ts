import { describe, it, expect, beforeEach } from 'vitest';
import { Readable } from 'stream';
import { rm } from 'fs/promises';
import { join } from 'path';

import { LocalStorageAdapter } from '../../src/infrastructure/adapters/local-storage-adapter';
import { RDIndex } from '../../src/core/models';

const TMP_DIR = join(__dirname, 'tmp');

function bufferToStream(data: Buffer): Readable {
  return Readable.from([data]);
}

describe('LocalStorageAdapter', () => {
  let adapter: LocalStorageAdapter;

  beforeEach(async () => {
    adapter = new LocalStorageAdapter({ basePath: TMP_DIR, type: 'local' });
    await rm(TMP_DIR, { recursive: true, force: true });
  });

  it('puts and gets a chunk', async () => {
    const hash = 'abc123';
    const data = Buffer.from('HelloWorld');

    await adapter.putChunk(hash, bufferToStream(data));
    const stream = await adapter.getChunk(hash);

    expect(stream).toBeDefined();

    let result = '';
    for await (const chunk of stream!) result += chunk.toString();

    expect(result).toBe('HelloWorld');
  });

  it('does not overwrite by default', async () => {
    const hash = 'abc123';
    const data1 = Buffer.from('Hello');
    const data2 = Buffer.from('World');

    await adapter.putChunk(hash, bufferToStream(data1));
    await expect(adapter.putChunk(hash, bufferToStream(data2))).rejects.toThrow(
      `Chunk already exists: ${hash}`
    );
  });

  it('overwrites when specified', async () => {
    const hash = 'abc123';
    const data1 = Buffer.from('Hello');
    const data2 = Buffer.from('World');

    await adapter.putChunk(hash, bufferToStream(data1));
    await adapter.putChunk(hash, bufferToStream(data2), { overwrite: true });

    const stream = await adapter.getChunk(hash);
    let result = '';
    for await (const chunk of stream!) result += chunk.toString();

    expect(result).toBe('World');
  });

  it('deletes a chunk', async () => {
    const hash = 'abc123';
    const data = Buffer.from('DeleteMe');

    await adapter.putChunk(hash, bufferToStream(data));
    await adapter.deleteChunk(hash);

    const exists = await adapter.chunkExists(hash);
    expect(exists).toBe(false);
  });

  it('lists chunks', async () => {
    const chunks = ['a', 'b', 'c'];
    for (const c of chunks) await adapter.putChunk(c, bufferToStream(Buffer.from(c)));

    const list = await adapter.listChunks();
    expect(list.sort()).toEqual(chunks.sort());
  });

  it('puts and gets remote index', async () => {
    const index: RDIndex = { chunkSize: 10, createdAt: Date.now(), version: 1, files: [] };
    await adapter.putRemoteIndex(index);

    const retrieved = await adapter.getRemoteIndex();
    expect(retrieved).toEqual(index);
  });

  it('getRemoteIndex returns null if missing', async () => {
    const result = await adapter.getRemoteIndex();
    expect(result).toBeNull();
  });
});
