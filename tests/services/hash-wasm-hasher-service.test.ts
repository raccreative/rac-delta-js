import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { writeFile, rm, mkdir } from 'fs/promises';
import { Readable } from 'stream';
import { join } from 'path';

import { HashWasmHasherService } from '../../src/infrastructure/services/hash-wasm-hasher-service';
import { UnknownAny } from '../../src/core/types';

const TMP_DIR = join(process.cwd(), 'tmp-hash-tests');
const FILE_PATH = join(TMP_DIR, 'test.txt');

describe('HashWasmHasherService', () => {
  const service = new HashWasmHasherService();
  const fileContent = Buffer.from('The quick brown fox jumps over the lazy dog');
  let fileHash: string;

  beforeAll(async () => {
    await rm(TMP_DIR, { recursive: true, force: true });
    await mkdir(TMP_DIR, { recursive: true });
    await writeFile(FILE_PATH, fileContent);
  });

  afterAll(async () => {
    await rm(TMP_DIR, { recursive: true, force: true });
  });

  it('should hash a buffer consistently', async () => {
    const hash1 = await service.hashBuffer(fileContent);
    const hash2 = await service.hashBuffer(fileContent);

    expect(hash1).toBe(hash2);

    fileHash = hash1;
  });

  it('should verify a chunk correctly', async () => {
    const valid = await service.verifyChunk(fileContent, fileHash);
    const invalid = await service.verifyChunk(fileContent, 'wronghash');

    expect(valid).toBe(true);
    expect(invalid).toBe(false);
  });

  it('should hash a file and produce valid metadata', async () => {
    const fileEntry = await service.hashFile('test.txt', TMP_DIR, 10);

    expect(fileEntry.path).toBe('test.txt');
    expect(fileEntry.size).toBe(fileContent.length);
    expect(fileEntry.hash).toBeTypeOf('string');
    expect(fileEntry.chunks.length).toBeGreaterThan(0);

    for (let i = 0; i < fileEntry.chunks.length; i++) {
      const chunk = fileEntry.chunks[i];
      expect(chunk.offset).toBe(i * 10);
      expect(chunk.size).toBeLessThanOrEqual(10);
    }
  });

  it('should verify a file hash correctly', async () => {
    const valid = await service.verifyFile(FILE_PATH, fileHash);
    const invalid = await service.verifyFile(FILE_PATH, 'deadbeef');

    expect(valid).toBe(true);
    expect(invalid).toBe(false);
  });

  it('should hash a stream and trigger onChunk callback', async () => {
    const data = Buffer.from('abcdef1234567890');
    const seen: Buffer[] = [];

    const readable = Readable.from([data]);
    const asyncStream: UnknownAny = readable as AsyncIterable<Uint8Array>;
    asyncStream.nextChunk = async () => data;
    asyncStream.close = async () => {};

    const chunks = await service.hashStream(asyncStream, (chunk) => seen.push(Buffer.from(chunk)));

    expect(chunks).toHaveLength(1);
    expect(chunks[0].size).toBe(data.length);
    expect(seen.length).toBe(1);
    expect(Buffer.compare(seen[0], data)).toBe(0);
  });

  it('should create a streaming hasher and produce consistent output', async () => {
    const hasher = await service.createStreamingHasher();
    hasher.update(Buffer.from('abc'));
    const digest1 = hasher.digest('hex');

    const hasher2 = await service.createStreamingHasher();
    hasher2.update(Buffer.from('abc'));
    const digest2 = hasher2.digest('hex');

    expect(digest1).toBe(digest2);
  });
});
