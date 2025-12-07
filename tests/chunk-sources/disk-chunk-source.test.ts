import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm, readdir, readFile } from 'fs/promises';
import { Readable } from 'stream';
import { tmpdir } from 'os';
import { join } from 'path';

import { DiskChunkSource } from '../../src/infrastructure/chunk-sources/disk-chunk-source';
import { ChunkNotFoundException } from '../../src/core/exceptions';

describe('DiskChunkSource', () => {
  let tempDir: string;
  let source: DiskChunkSource;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'disk-source-test-'));
    source = new DiskChunkSource(tempDir);
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  it('stores and retrieves a chunk as Buffer', async () => {
    const hash = 'chunk1';
    const data = Buffer.from('hello world');

    await source.setChunk(hash, data);
    const retrieved = await source.getChunk(hash);

    expect(retrieved.equals(data)).toBe(true);
  });

  it('stores a chunk from a Readable stream', async () => {
    const hash = 'streamed';
    const stream = Readable.from(['streamed data']);

    await source.setChunk(hash, stream);
    const retrieved = await source.getChunk(hash);

    expect(retrieved.toString()).toBe('streamed data');
  });

  it('throws an error if chunk does not exist', async () => {
    await expect(source.getChunk('missing')).rejects.toThrow(ChunkNotFoundException);
  });

  it('returns true if a chunk exists', async () => {
    const hash = 'exists';
    const data = Buffer.from('exists data');

    await source.setChunk(hash, data);
    const exists = await source.hasChunk(hash);

    expect(exists).toBe(true);
  });

  it('returns false if a chunk does not exist', async () => {
    const exists = await source.hasChunk('not-there');
    expect(exists).toBe(false);
  });

  it('retrieves multiple chunks via getChunks()', async () => {
    const chunks = {
      a: Buffer.from('aaa'),
      b: Buffer.from('bbb'),
      c: Buffer.from('ccc'),
    };

    for (const [hash, data] of Object.entries(chunks)) {
      await source.setChunk(hash, data);
    }

    const result = await source.getChunks(['a', 'b', 'c']);

    expect(result.size).toBe(3);
    expect(result.get('a')?.equals(chunks.a)).toBe(true);
    expect(result.get('b')?.equals(chunks.b)).toBe(true);
    expect(result.get('c')?.equals(chunks.c)).toBe(true);
  });

  it('throws if one requested chunk in getChunks() is missing', async () => {
    const hash = 'a';
    await source.setChunk(hash, Buffer.from('data'));

    await expect(source.getChunks([hash, 'missing'])).rejects.toThrow(ChunkNotFoundException);
  });

  it('clears all stored chunks with clear()', async () => {
    await source.setChunk('to-delete', Buffer.from('data'));

    let files = await readdir(tempDir);
    expect(files.length).toBeGreaterThan(0);

    await source.clear();

    let cleared: string[] = [];
    try {
      cleared = await readdir(tempDir);
    } catch {
      cleared = [];
    }

    expect(cleared.length).toBe(0);
  });

  it('creates nested directories automatically when setting a chunk', async () => {
    const nestedHash = 'nested/dir/chunk';
    const data = Buffer.from('nested data');

    await source.setChunk(nestedHash, data);
    const retrieved = await readFile(join(tempDir, nestedHash));

    expect(retrieved.equals(data)).toBe(true);
  });
});
