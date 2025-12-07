import { describe, test, expect, beforeEach, afterEach } from 'vitest';
import { mkdir, writeFile, rm, readFile, access } from 'fs/promises';
import { Readable } from 'stream';
import { tmpdir } from 'os';
import { join } from 'path';

import { DefaultHashDownloadPipeline } from '../../src/infrastructure/pipelines/default-hash-download-pipeline';
import { MemoryReconstructionService } from '../../src/infrastructure/services/memory-reconstruction-service';
import { MemoryValidationService } from '../../src/infrastructure/services/memory-validation-service';
import { HashWasmHasherService } from '../../src/infrastructure/services/hash-wasm-hasher-service';
import { LocalStorageAdapter } from '../../src/infrastructure/adapters/local-storage-adapter';
import { MemoryDeltaService } from '../../src/infrastructure/services/memory-delta-service';
import { RDIndex } from '../../src/core/models';
import { UpdateStrategy } from '../../src/core/pipelines';

describe('DefaultHashDownloadPipeline', () => {
  let tmp: string;
  let storage: LocalStorageAdapter;
  let hasher: HashWasmHasherService;
  let reconstruction: MemoryReconstructionService;
  let validation: MemoryValidationService;
  let delta: MemoryDeltaService;
  let pipeline: DefaultHashDownloadPipeline;

  beforeEach(async () => {
    tmp = join(tmpdir(), 'pipeline-test');
    await mkdir(tmp, { recursive: true });

    storage = new LocalStorageAdapter({ basePath: tmp, type: 'local' });
    hasher = new HashWasmHasherService();
    reconstruction = new MemoryReconstructionService(hasher);
    validation = new MemoryValidationService(hasher);
    delta = new MemoryDeltaService(hasher);

    pipeline = new DefaultHashDownloadPipeline(
      reconstruction,
      validation,
      storage,
      { maxConcurrency: 2, chunkSize: 1024, storage: { type: 'local', basePath: tmp } },
      delta
    );
  });

  afterEach(async () => {
    await rm(tmp, { recursive: true, force: true });
  });

  const createChunk = async (content: string) => {
    const buffer = Buffer.from(content, 'utf-8');
    const hash = await hasher.hashBuffer(buffer);
    const stream = Readable.from([buffer]);
    await storage.putChunk(hash, stream);
    return { hash, content };
  };

  test('downloads and reconstructs file in memory strategy', async () => {
    const chunk = await createChunk('hello world');

    const remoteIndex: RDIndex = {
      chunkSize: 10,
      createdAt: Date.now(),
      version: 1,
      files: [
        {
          path: 'file.txt',
          size: chunk.content.length,
          modifiedAt: Date.now(),
          hash: chunk.hash,
          chunks: [{ hash: chunk.hash, size: chunk.content.length, offset: 0 }],
        },
      ],
    };

    await pipeline.execute(tmp, UpdateStrategy.DownloadAllFirstToMemory, remoteIndex);

    const fileContent = await readFile(join(tmp, 'file.txt'), 'utf-8');
    expect(fileContent).toBe(chunk.content);

    // Index should exist
    const indexContent = await readFile(join(tmp, 'rd-index.json'), 'utf-8');
    const indexJson = JSON.parse(indexContent);
    expect(indexJson.files.length).toBe(1);
  });

  test('downloads and reconstructs file in disk strategy', async () => {
    const chunk = await createChunk('disk strategy test');

    const remoteIndex: RDIndex = {
      chunkSize: 10,
      createdAt: Date.now(),
      version: 1,
      files: [
        {
          path: 'disk-file.txt',
          modifiedAt: Date.now(),
          size: chunk.content.length,
          hash: chunk.hash,
          chunks: [{ hash: chunk.hash, size: chunk.content.length, offset: 0 }],
        },
      ],
    };

    await pipeline.execute(tmp, UpdateStrategy.DownloadAllFirstToDisk, remoteIndex, {
      chunksSavePath: tmp,
    });

    const fileContent = await readFile(join(tmp, 'disk-file.txt'), 'utf-8');
    expect(fileContent).toBe(chunk.content);
  });

  test('downloads and reconstructs file streaming from network', async () => {
    const chunk = await createChunk('streaming test');

    const remoteIndex: RDIndex = {
      chunkSize: 10,
      createdAt: Date.now(),
      version: 1,
      files: [
        {
          path: 'stream-file.txt',
          size: chunk.content.length,
          modifiedAt: Date.now(),
          hash: chunk.hash,
          chunks: [{ hash: chunk.hash, size: chunk.content.length, offset: 0 }],
        },
      ],
    };

    await pipeline.execute(tmp, UpdateStrategy.StreamFromNetwork, remoteIndex);

    const fileContent = await readFile(join(tmp, 'stream-file.txt'), 'utf-8');
    expect(fileContent).toBe(chunk.content);
  });

  test('cleans obsolete chunks', async () => {
    const oldFilePath = join(tmp, 'old-file.txt');
    await writeFile(oldFilePath, 'obsolete content');

    const remoteIndex: RDIndex = { chunkSize: 10, version: 1, createdAt: Date.now(), files: [] }; // removed from remote
    const plan = await delta.compareForDownload(null, remoteIndex);
    plan.obsoleteChunks.push({ filePath: 'old-file.txt', hash: 'dummy', size: 0, offset: 0 });

    const result = await pipeline.verifyAndDeleteObsoleteChunks(
      plan,
      tmp,
      remoteIndex,
      null as any
    );
    expect(result.deletedFiles).toContain('old-file.txt');
    expect(
      await access(oldFilePath)
        .then(() => true)
        .catch(() => false)
    ).toBe(false);
  });

  test('uses existing local index if useExistingIndex true', async () => {
    const indexPath = join(tmp, 'rd-index.json');
    const fakeIndex = { files: [] };
    await writeFile(indexPath, JSON.stringify(fakeIndex));

    const foundIndex = await pipeline.findLocalIndex(tmp);
    expect(foundIndex).toEqual(fakeIndex);
  });
});
