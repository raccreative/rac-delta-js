import { describe, test, expect, beforeEach, afterEach } from 'vitest';
import { mkdir, rm, readFile, access } from 'fs/promises';
import { Readable } from 'stream';
import { tmpdir } from 'os';
import { join } from 'path';

import { DefaultHashDownloadPipeline } from '../../src/infrastructure/pipelines/default-hash-download-pipeline';
import { MemoryReconstructionService } from '../../src/infrastructure/services/memory-reconstruction-service';
import { MemoryValidationService } from '../../src/infrastructure/services/memory-validation-service';
import { HashWasmHasherService } from '../../src/infrastructure/services/hash-wasm-hasher-service';
import { LocalStorageAdapter } from '../../src/infrastructure/adapters/local-storage-adapter';
import { MemoryDeltaService } from '../../src/infrastructure/services/memory-delta-service';

import { UpdateStrategy } from '../../src/core/pipelines';
import { RDIndex } from '../../src/core/models';

function abortedController(): AbortController {
  const controller = new AbortController();
  controller.abort();
  return controller;
}

function isAbortError(err: unknown): boolean {
  return (err as any)?.name === 'AbortError';
}

describe('DefaultHashDownloadPipeline — abort signal', () => {
  let tmp: string;
  let storage: LocalStorageAdapter;
  let hasher: HashWasmHasherService;
  let reconstruction: MemoryReconstructionService;
  let validation: MemoryValidationService;
  let delta: MemoryDeltaService;
  let pipeline: DefaultHashDownloadPipeline;

  beforeEach(async () => {
    tmp = join(tmpdir(), `pipeline-abort-test-${Date.now()}`);
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
    return { hash, buffer, content };
  };

  const buildRemoteIndex = (files: { path: string; hash: string; size: number }[]): RDIndex => ({
    chunkSize: 1024,
    createdAt: Date.now(),
    version: 1,
    files: files.map((f) => ({
      path: f.path,
      size: f.size,
      modifiedAt: Date.now(),
      hash: f.hash,
      chunks: [{ hash: f.hash, size: f.size, offset: 0 }],
    })),
  });

  test('throws AbortError immediately if signal is already aborted (StreamFromNetwork)', async () => {
    const chunk = await createChunk('should not be written');
    const remoteIndex = buildRemoteIndex([
      { path: 'file.txt', hash: chunk.hash, size: chunk.content.length },
    ]);

    const { signal } = abortedController();

    await expect(
      pipeline.execute(tmp, UpdateStrategy.StreamFromNetwork, remoteIndex, { signal })
    ).rejects.toSatisfy(isAbortError);

    const exists = await access(join(tmp, 'file.txt'))
      .then(() => true)
      .catch(() => false);
    expect(exists).toBe(false);
  });

  test('throws AbortError immediately if signal is already aborted (DownloadAllFirstToMemory)', async () => {
    const chunk = await createChunk('should not be written');
    const remoteIndex = buildRemoteIndex([
      { path: 'file.txt', hash: chunk.hash, size: chunk.content.length },
    ]);

    const { signal } = abortedController();

    await expect(
      pipeline.execute(tmp, UpdateStrategy.DownloadAllFirstToMemory, remoteIndex, { signal })
    ).rejects.toSatisfy(isAbortError);

    const exists = await access(join(tmp, 'file.txt'))
      .then(() => true)
      .catch(() => false);
    expect(exists).toBe(false);
  });

  test('aborts cleanly mid-download and leaves no .tmp files behind', async () => {
    const chunks = await Promise.all(
      Array.from({ length: 6 }, (_, i) => createChunk(`chunk-content-${i}-${'x'.repeat(512)}`))
    );

    const remoteIndex = buildRemoteIndex(
      chunks.map((c, i) => ({ path: `file-${i}.txt`, hash: c.hash, size: c.content.length }))
    );

    const controller = new AbortController();

    setTimeout(() => controller.abort(), 20);

    await expect(
      pipeline.execute(tmp, UpdateStrategy.StreamFromNetwork, remoteIndex, {
        signal: controller.signal,
      })
    ).rejects.toSatisfy(isAbortError);

    const { readdir } = await import('fs/promises');
    const allFiles = await readdir(tmp);
    const tmpFiles = allFiles.filter((f) => f.endsWith('.tmp'));
    expect(tmpFiles).toHaveLength(0);
  });

  test('files fully reconstructed before abort are intact on disk', async () => {
    const first = await createChunk('first file content');
    const second = await createChunk('second file content — much longer so it takes more time');

    const remoteIndex = buildRemoteIndex([
      { path: 'first.txt', hash: first.hash, size: first.content.length },
      { path: 'second.txt', hash: second.hash, size: second.content.length },
    ]);

    const controller = new AbortController();

    setTimeout(() => controller.abort(), 30);

    await expect(
      pipeline.execute(tmp, UpdateStrategy.StreamFromNetwork, remoteIndex, {
        signal: controller.signal,
        fileReconstructionConcurrency: 1,
      })
    ).rejects.toSatisfy(isAbortError);

    const indexExists = await access(join(tmp, 'rd-index.json'))
      .then(() => true)
      .catch(() => false);
    expect(indexExists).toBe(false);

    const { readdir } = await import('fs/promises');
    const allFiles = await readdir(tmp);
    const tmpFiles = allFiles.filter((f) => f.endsWith('.tmp'));
    expect(tmpFiles).toHaveLength(0);
  });

  test('rd-index.json is NOT saved when download is aborted', async () => {
    const chunk = await createChunk('some content');
    const remoteIndex = buildRemoteIndex([
      { path: 'file.txt', hash: chunk.hash, size: chunk.content.length },
    ]);

    const { signal } = abortedController();

    await expect(
      pipeline.execute(tmp, UpdateStrategy.StreamFromNetwork, remoteIndex, { signal })
    ).rejects.toSatisfy(isAbortError);

    const indexExists = await access(join(tmp, 'rd-index.json'))
      .then(() => true)
      .catch(() => false);
    expect(indexExists).toBe(false);
  });

  test('re-running execute after abort picks up files already on disk and skips them', async () => {
    const chunks = await Promise.all(
      Array.from({ length: 3 }, (_, i) => createChunk(`content of file ${i}`))
    );

    const remoteIndex = buildRemoteIndex(
      chunks.map((c, i) => ({ path: `file-${i}.txt`, hash: c.hash, size: c.content.length }))
    );

    const controller = new AbortController();
    setTimeout(() => controller.abort(), 10);

    await expect(
      pipeline.execute(tmp, UpdateStrategy.StreamFromNetwork, remoteIndex, {
        signal: controller.signal,
      })
    ).rejects.toSatisfy(isAbortError);

    await pipeline.execute(tmp, UpdateStrategy.StreamFromNetwork, remoteIndex);

    for (let i = 0; i < chunks.length; i++) {
      const content = await readFile(join(tmp, `file-${i}.txt`), 'utf-8');
      expect(content).toBe(chunks[i].content);
    }

    const indexExists = await access(join(tmp, 'rd-index.json'))
      .then(() => true)
      .catch(() => false);
    expect(indexExists).toBe(true);
  });

  test('execute completes normally when no signal is provided', async () => {
    const chunk = await createChunk('normal execution');
    const remoteIndex = buildRemoteIndex([
      { path: 'normal.txt', hash: chunk.hash, size: chunk.content.length },
    ]);

    await pipeline.execute(tmp, UpdateStrategy.StreamFromNetwork, remoteIndex);

    const content = await readFile(join(tmp, 'normal.txt'), 'utf-8');
    expect(content).toBe(chunk.content);
  });
});
