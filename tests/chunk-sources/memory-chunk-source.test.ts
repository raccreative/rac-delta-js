import { describe, it, expect } from 'vitest';

import { MemoryChunkSource } from '../../src/infrastructure/chunk-sources/memory-chunk-source';
import { ChunkNotFoundException } from '../../src/core/exceptions';

describe('MemoryChunkSource', () => {
  it('stores and retrieves a single chunk', async () => {
    const source = new MemoryChunkSource();
    const hash = 'abc123';
    const data = Buffer.from('hello world');

    source.setChunk(hash, data);
    const retrieved = await source.getChunk(hash);

    expect(retrieved.equals(data)).toBe(true);
  });

  it('throws an error when requesting a missing chunk', async () => {
    const source = new MemoryChunkSource();

    await expect(source.getChunk('missing')).rejects.toThrow(ChunkNotFoundException);
  });

  it('checks existence of chunks correctly', () => {
    const source = new MemoryChunkSource();
    source.setChunk('exists', Buffer.from('data'));

    expect(source.hasChunk('exists')).toBe(true);
    expect(source.hasChunk('nope')).toBe(false);
  });

  it('retrieves multiple chunks with getChunks()', async () => {
    const source = new MemoryChunkSource();

    const chunks = {
      a: Buffer.from('one'),
      b: Buffer.from('two'),
      c: Buffer.from('three'),
    };

    for (const [hash, buf] of Object.entries(chunks)) {
      source.setChunk(hash, buf);
    }

    const result = await source.getChunks(['a', 'b', 'c']);

    expect(result.size).toBe(3);
    expect(result.get('a')?.equals(chunks.a)).toBe(true);
    expect(result.get('b')?.equals(chunks.b)).toBe(true);
    expect(result.get('c')?.equals(chunks.c)).toBe(true);
  });

  it('throws if one of the requested chunks in getChunks() is missing', async () => {
    const source = new MemoryChunkSource();
    source.setChunk('a', Buffer.from('data'));

    await expect(source.getChunks(['a', 'b'])).rejects.toThrow(ChunkNotFoundException);
  });

  it('overwrites existing chunks when setChunk() is called again', async () => {
    const source = new MemoryChunkSource();
    const hash = 'same';
    const first = Buffer.from('old');
    const second = Buffer.from('new');

    source.setChunk(hash, first);
    source.setChunk(hash, second);

    const retrieved = await source.getChunk(hash);
    expect(retrieved.equals(second)).toBe(true);
  });
});
