import { describe, it, expect, beforeAll } from 'vitest';

import { MemoryDeltaService } from '../../src/infrastructure/services/memory-delta-service';
import { HashWasmHasherService } from '../../src/infrastructure/services/hash-wasm-hasher-service';

import { AsyncChunkStream } from '../../src/core/services';

describe('MemoryDeltaService', () => {
  let service: MemoryDeltaService;

  beforeAll(() => {
    service = new MemoryDeltaService(new HashWasmHasherService());
  });

  it('should create a FileEntry from stream', async () => {
    const data = Buffer.from('HelloWorld');

    const stream: AsyncChunkStream = {
      [Symbol.asyncIterator]: async function* () {
        yield data;
      },
      nextChunk: async () => data,
      close: async () => {},
    };

    const hasher = new HashWasmHasherService();
    const service = new MemoryDeltaService(hasher);

    const fileEntry = await service.createFileEntryFromStream(stream, 'test.txt');

    expect(fileEntry.path).toBe('test.txt');
    expect(fileEntry.size).toBe(data.length);
    expect(fileEntry.chunks.length).toBe(1);
    expect(fileEntry.chunks[0].size).toBe(data.length);

    const expectedHash = await hasher.hashBuffer(data);
    expect(fileEntry.hash).toBe(expectedHash);
  });

  it('should merge delta plans without duplicates', () => {
    const planA = {
      deletedFiles: ['a.txt'],
      missingChunks: [{ hash: 'c1', offset: 0, filePath: 'a.txt' }],
      obsoleteChunks: [],
      newAndModifiedFiles: [],
    };

    const planB = {
      deletedFiles: ['a.txt', 'b.txt'],
      missingChunks: [
        { hash: 'c1', offset: 0, filePath: 'a.txt' },
        { hash: 'c2', offset: 0, filePath: 'b.txt' },
      ],
      obsoleteChunks: [],
      newAndModifiedFiles: [],
    };

    const merged = service.mergePlans(planA as any, planB as any);

    expect(merged.deletedFiles).toEqual(['a.txt', 'b.txt']);
    expect(merged.missingChunks.length).toBe(2);
  });

  it('should compare indices and produce missing chunks', () => {
    const sourceIndex = {
      version: 1,
      createdAt: Date.now(),
      chunkSize: 5,
      files: [
        {
          path: 'file1.txt',
          size: 10,
          hash: 'filehash',
          modifiedAt: Date.now(),
          chunks: [
            { hash: 'c1', offset: 0, size: 5 },
            { hash: 'c2', offset: 5, size: 5 },
          ],
        },
      ],
    };
    const targetIndex = {
      ...sourceIndex,
      files: [],
    };

    const deltaPlan = service.compare(sourceIndex as any, targetIndex as any);
    expect(deltaPlan.missingChunks.length).toBe(2);
    expect(deltaPlan.newAndModifiedFiles.length).toBe(1);
  });
});
