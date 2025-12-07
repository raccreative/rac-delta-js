import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdir, rm } from 'fs/promises';
import { Readable } from 'stream';
import { join } from 'path';

import { LocalStorageAdapter } from '../../src/infrastructure/adapters/local-storage-adapter';
import { StorageChunkSource } from '../../src/infrastructure/chunk-sources/storage-chunk-source';
import { ChunkNotFoundException } from '../../src/core/exceptions';
import { streamToBuffer } from '../../src/core/utils/stream-to-buffer';

function bufferToStream(buf: Buffer): Readable {
  const stream = new Readable();
  stream.push(buf);
  stream.push(null);
  return stream;
}

describe('StorageChunkSource + LocalStorageAdapter', () => {
  const basePath = join(__dirname, 'test-storage');
  let adapter: LocalStorageAdapter;
  let source: StorageChunkSource;

  beforeEach(async () => {
    adapter = new LocalStorageAdapter({ basePath, type: 'local' });
    source = new StorageChunkSource(adapter);
    await mkdir(basePath, { recursive: true });
  });

  afterEach(async () => {
    await rm(basePath, { recursive: true, force: true });
  });

  it('retrieves a chunk successfully', async () => {
    const hash = 'abc123';
    const data = Buffer.from('hello world');

    await adapter.putChunk(hash, bufferToStream(data), { overwrite: true });

    const result = await source.getChunk(hash);
    expect(result).toEqual(data);
  });

  it('throws if chunk does not exist', async () => {
    const hash = 'doesnotexist';
    await expect(source.getChunk(hash)).rejects.toThrow(ChunkNotFoundException);
  });

  it('gets multiple chunks with getChunks', async () => {
    const hashes = ['a', 'b', 'c'];
    for (const hash of hashes) {
      await adapter.putChunk(hash, bufferToStream(Buffer.from(hash)), { overwrite: true });
    }

    const resultMap = await source.getChunks(hashes);
    expect(resultMap.get('a')?.toString()).toBe('a');
    expect(resultMap.get('b')?.toString()).toBe('b');
    expect(resultMap.get('c')?.toString()).toBe('c');
  });

  it('streams chunks in order', async () => {
    const hashes = ['x', 'y'];
    await adapter.putChunk('x', bufferToStream(Buffer.from('X')), { overwrite: true });
    await adapter.putChunk('y', bufferToStream(Buffer.from('Y')), { overwrite: true });

    const result: string[] = [];
    for await (const { data } of source.streamChunks(hashes, { preserveOrder: true })) {
      const buffer = Buffer.isBuffer(data) ? data : await streamToBuffer(data);
      result.push(buffer.toString());
    }

    expect(result).toEqual(['X', 'Y']);
  });
});
