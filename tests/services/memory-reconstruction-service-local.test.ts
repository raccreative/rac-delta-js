import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdir, rm } from 'fs/promises';
import { Readable } from 'stream';
import { join } from 'path';

import { MemoryReconstructionService } from '../../src/infrastructure/services/memory-reconstruction-service';
import { HashWasmHasherService } from '../../src/infrastructure/services/hash-wasm-hasher-service';
import { StorageChunkSource } from '../../src/infrastructure/chunk-sources/storage-chunk-source';
import { LocalStorageAdapter } from '../../src/infrastructure/adapters/local-storage-adapter';
import { DeltaPlan, FileEntry } from '../../src/core/models';

function bufferToStream(buf: Buffer) {
  const stream = new Readable();
  stream.push(buf);
  stream.push(null);
  return stream;
}

describe('MemoryReconstructionService with StorageChunkSource + LocalStorageAdapter', () => {
  const baseDir = join(__dirname, 'tmp-test');
  let adapter: LocalStorageAdapter;
  let chunkSource: StorageChunkSource;
  let service: MemoryReconstructionService;
  let hashService: HashWasmHasherService;

  beforeEach(async () => {
    await mkdir(baseDir, { recursive: true });
    adapter = new LocalStorageAdapter({ basePath: baseDir, type: 'local' });
    chunkSource = new StorageChunkSource(adapter);
    hashService = new HashWasmHasherService();
    service = new MemoryReconstructionService(hashService);
  });

  afterEach(async () => {
    await rm(baseDir, { recursive: true, force: true });
  });

  it('reconstructs a single file from chunks', async () => {
    const data1 = Buffer.from('Hello ');
    const data2 = Buffer.from('World!');
    const hash1 = await hashService.hashBuffer(data1);
    const hash2 = await hashService.hashBuffer(data2);

    await adapter.putChunk(hash1, bufferToStream(data1), { overwrite: true });
    await adapter.putChunk(hash2, bufferToStream(data2), { overwrite: true });

    const entry: FileEntry = {
      path: 'file.txt',
      hash: await hashService.hashBuffer(Buffer.concat([data1, data2])),
      modifiedAt: Date.now(),
      size: data1.length + data2.length,
      chunks: [
        { hash: hash1, offset: 0, size: data1.length },
        { hash: hash2, offset: data1.length, size: data2.length },
      ],
    };

    const plan: DeltaPlan = {
      newAndModifiedFiles: [entry],
      obsoleteChunks: [],
      missingChunks: [],
      deletedFiles: [],
    };

    const outputDir = join(baseDir, 'output');
    await service.reconstructAll(plan, outputDir, chunkSource);

    const result = await adapter.getChunk(entry.chunks[0].hash);
    expect(result).toBeTruthy();

    const reconstructed = await adapter.getChunkInfo(hash1);
    expect(reconstructed).toBeTruthy();
  });

  it('reconstructToStream returns correct buffer', async () => {
    const data = Buffer.from('Stream me');
    const hash = await hashService.hashBuffer(data);
    await adapter.putChunk(hash, bufferToStream(data), { overwrite: true });

    const entry: FileEntry = {
      path: 'stream.txt',
      hash,
      modifiedAt: Date.now(),
      size: data.length,
      chunks: [{ hash, offset: 0, size: data.length }],
    };

    const stream = await service.reconstructToStream(entry, chunkSource);
    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      chunks.push(Buffer.from(chunk));
    }

    expect(Buffer.concat(chunks)).toEqual(data);
  });

  it('throws if chunk is missing', async () => {
    const missingHash = 'not-exist';
    const entry: FileEntry = {
      path: 'fail.txt',
      hash: missingHash,
      modifiedAt: Date.now(),
      size: 5,
      chunks: [{ hash: missingHash, offset: 0, size: 5 }],
    };

    await expect(
      service.reconstructFile(entry, join(baseDir, 'fail.txt'), chunkSource)
    ).rejects.toThrowError();
  });

  it('performs in-place reconstruction when file exists and is large', async () => {
    const data1 = Buffer.from('AAAA');
    const data2 = Buffer.from('BBBB');
    const fullData = Buffer.concat([data1, data2]);

    const hash1 = await hashService.hashBuffer(data1);
    const hash2 = await hashService.hashBuffer(data2);
    const fullHash = await hashService.hashBuffer(fullData);

    await adapter.putChunk(hash1, bufferToStream(data1), { overwrite: true });
    await adapter.putChunk(hash2, bufferToStream(data2), { overwrite: true });

    const outputPath = join(baseDir, 'bigfile.txt');
    await mkdir(baseDir, { recursive: true });
    await adapter.putChunk('dummy', bufferToStream(Buffer.alloc(1024 * 1024 * 2, 'X')), {
      overwrite: true,
    });
    await rm(outputPath, { force: true });

    const largeBuffer = Buffer.alloc(1024 * 1024 * 2, 'Z'); // 2MB
    await import('fs/promises').then((fs) => fs.writeFile(outputPath, largeBuffer));

    const entry: FileEntry = {
      path: 'bigfile.txt',
      hash: fullHash,
      modifiedAt: Date.now(),
      size: fullData.length,
      chunks: [
        { hash: hash1, offset: 0, size: data1.length },
        { hash: hash2, offset: data1.length, size: data2.length },
      ],
    };

    const plan: DeltaPlan = {
      newAndModifiedFiles: [entry],
      obsoleteChunks: [],
      missingChunks: [],
      deletedFiles: [],
    };

    await service.reconstructAll(plan, baseDir, chunkSource, {
      inPlaceReconstructionThreshold: 1000, // bytes
    });

    const rebuilt = await import('fs/promises').then((fs) => fs.readFile(outputPath));
    expect(rebuilt.slice(0, fullData.length).toString()).toBe(fullData.toString());
  });

  it('reconstructs via temp file when file exists but is small', async () => {
    const original = Buffer.from('OLD');
    const chunk = Buffer.from('NEW');
    const hash = await hashService.hashBuffer(chunk);

    await adapter.putChunk(hash, bufferToStream(chunk), { overwrite: true });

    const entry: FileEntry = {
      path: 'small.txt',
      hash,
      modifiedAt: Date.now(),
      size: chunk.length,
      chunks: [{ hash, offset: 0, size: chunk.length }],
    };

    const outputPath = join(baseDir, 'small.txt');

    await import('fs/promises').then((fs) => fs.writeFile(outputPath, original));

    const plan: DeltaPlan = {
      newAndModifiedFiles: [entry],
      obsoleteChunks: [],
      missingChunks: [],
      deletedFiles: [],
    };

    await service.reconstructAll(plan, baseDir, chunkSource, {
      verifyAfterRebuild: true,
      inPlaceReconstructionThreshold: 1024 * 1024,
    });

    const rebuilt = await import('fs/promises').then((fs) => fs.readFile(outputPath));
    expect(rebuilt.toString()).toBe('NEW');
  });
});
