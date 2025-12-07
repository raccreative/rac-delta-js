import { describe, it, expect, beforeEach } from 'vitest';
import { writeFile, mkdir, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

import { DefaultHashUploadPipeline } from '../../src/infrastructure/pipelines/default-hash-upload-pipeline';
import { HashWasmHasherService } from '../../src/infrastructure/services/hash-wasm-hasher-service';
import { LocalStorageAdapter } from '../../src/infrastructure/adapters/local-storage-adapter';
import { MemoryDeltaService } from '../../src/infrastructure/services/memory-delta-service';

const baseTmp = join(tmpdir(), `upload-tests-${Date.now()}`);

describe('DefaultHashUploadPipeline', () => {
  let tmp: string;
  let storage: LocalStorageAdapter;
  let delta: MemoryDeltaService;
  let pipeline: DefaultHashUploadPipeline;
  const hasher = new HashWasmHasherService();

  beforeEach(async () => {
    tmp = join(baseTmp, `case-${Date.now()}`);
    await rm(tmp, { recursive: true, force: true });
    await mkdir(tmp, { recursive: true });

    storage = new LocalStorageAdapter({ basePath: tmp, type: 'local' });
    delta = new MemoryDeltaService(hasher);
    pipeline = new DefaultHashUploadPipeline(storage, delta, {
      chunkSize: 5,
      maxConcurrency: 2,
      storage: { type: 'local', basePath: tmp },
    });
  });

  const createFile = async (name: string, content: string) => {
    const path = join(tmp, name);
    await mkdir(join(path, '..'), { recursive: true });
    await writeFile(path, content, 'utf-8');
    return { path, content, name };
  };

  it('uploads all chunks and remote index', async () => {
    const file = await createFile('file.txt', 'hello world');

    const result = await pipeline.execute(tmp);

    const fileIndex = result.files.find((f) => f.path === 'file.txt');
    expect(fileIndex).toBeDefined();

    for (const chunk of fileIndex!.chunks) {
      const stream = await storage.getChunk(chunk.hash);
      const buffers: Buffer[] = [];

      for await (const b of stream!) {
        buffers.push(Buffer.from(b));
      }

      const content = Buffer.concat(buffers).toString('utf-8');
      expect(content).toBe(file.content.slice(chunk.offset, chunk.offset + chunk.size));
    }

    const remoteIndex = await storage.getRemoteIndex();
    expect(remoteIndex).toBeDefined();
    expect(remoteIndex!.files.some((f) => f.path === 'file.txt')).toBe(true);
  });

  it('uploads multiple files with multiple chunks', async () => {
    const file1 = await createFile('file1.txt', 'abcdefghij12345');
    const file2 = await createFile('file2.txt', '9876543210xyz');

    const result = await pipeline.execute(tmp);

    for (const file of [file1, file2]) {
      const fileIndex = result.files.find((f) => f.path === file.name);
      expect(fileIndex).toBeDefined();

      for (const chunk of fileIndex!.chunks) {
        const stream = await storage.getChunk(chunk.hash);
        const buffers: Buffer[] = [];

        for await (const b of stream!) {
          buffers.push(Buffer.from(b));
        }

        const content = Buffer.concat(buffers).toString('utf-8');
        expect(content).toBe(file.content.slice(chunk.offset, chunk.offset + chunk.size));
      }
    }
  });

  it('uploads everything if force option is set', async () => {
    const file = await createFile('force.txt', 'force content');

    const result = await pipeline.execute(tmp, undefined, {
      force: true,
      ignorePatterns: ['chunks/*'],
    });

    const fileIndex = result.files.find((f) => f.path === 'force.txt');
    expect(fileIndex).toBeDefined();

    for (const chunk of fileIndex!.chunks) {
      const stream = await storage.getChunk(chunk.hash);
      const buffers: Buffer[] = [];

      for await (const b of stream!) {
        buffers.push(Buffer.from(b));
      }

      const content = Buffer.concat(buffers).toString('utf-8');
      expect(content).toBe(file.content.slice(chunk.offset, chunk.offset + chunk.size));
    }
  });

  it('ignores files matching glob patterns', async () => {
    await createFile('include.txt', 'ok');
    await createFile('ignore.ts', 'ignored');
    await mkdir(join(tmp, 'node_modules'));
    await createFile('node_modules/test.js', 'ignored');

    const index = await delta.createIndexFromDirectory(tmp, 1024, 4, [
      '*.ts',
      '**/node_modules/**',
      'chunks/*',
    ]);
    const paths = index.files.map((f) => f.path);

    expect(paths).toContain('include.txt');
    expect(paths).not.toContain('ignore.ts');
    expect(paths).not.toContain('node_modules/test.js');
  });

  it('throw error if requireRemoteIndex and no remote index', async () => {
    await createFile('include.txt', 'ok');

    await expect(
      pipeline.execute(tmp, undefined, { requireRemoteIndex: true, force: false })
    ).rejects.toThrowError();
  });

  it('does not delete shared chunks when used in multiple files', async () => {
    const contentShared = 'ABCDE';
    const contentA = contentShared + '12345';
    const contentB = contentShared + '67890';

    const localDir = join(baseTmp, `local-${Date.now()}`);
    const remoteDir = join(baseTmp, `remote-${Date.now()}`);
    await mkdir(localDir, { recursive: true });
    await mkdir(remoteDir, { recursive: true });

    const storageRemote = new LocalStorageAdapter({ basePath: remoteDir, type: 'local' });
    const deltaService = new MemoryDeltaService(hasher);
    const pipelineRemote = new DefaultHashUploadPipeline(storageRemote, deltaService, {
      chunkSize: 5,
      maxConcurrency: 2,
      storage: { type: 'local', basePath: remoteDir },
    });

    const createLocalFile = async (name: string, content: string) => {
      const path = join(localDir, name);
      await mkdir(join(path, '..'), { recursive: true });
      await writeFile(path, content, 'utf-8');
      return { path, content, name };
    };

    await createLocalFile('A.txt', contentA);
    await createLocalFile('B.txt', contentB);

    const firstIndex = await pipelineRemote.execute(localDir);

    const sharedChunk = firstIndex.files
      .flatMap((f) => f.chunks)
      .find((c) => contentShared.includes(contentA.slice(c.offset, c.offset + c.size)));

    expect(sharedChunk).toBeDefined();

    await writeFile(join(localDir, 'A.txt'), '12345', 'utf-8');

    const newIndex = await pipelineRemote.execute(localDir, firstIndex);

    const exists = await storageRemote.chunkExists(sharedChunk!.hash);
    expect(exists).toBe(true);

    const updatedFileA = newIndex.files.find((f) => f.path === 'A.txt');
    expect(updatedFileA).toBeDefined();
  });

  it('deletes obsolete chunks when remote index exists', async () => {
    await createFile('file.txt', '1234567890');
    const firstIndex = await pipeline.execute(tmp);

    const remoteStorage = storage;

    const tmp2 = join(baseTmp, `case2-${Date.now()}`);
    await mkdir(tmp2, { recursive: true });

    const storage2 = new LocalStorageAdapter({ basePath: tmp, type: 'local' });
    const delta2 = new MemoryDeltaService(new HashWasmHasherService());
    const pipeline2 = new DefaultHashUploadPipeline(storage2, delta2, {
      chunkSize: 5,
      maxConcurrency: 2,
      storage: { type: 'local', basePath: tmp },
    });

    const newIndex = await pipeline2.execute(tmp2, firstIndex);

    for (const oldFile of firstIndex.files) {
      for (const chunk of oldFile.chunks) {
        const chunkPath = join(tmp, 'chunks', chunk.hash);
        const exists = await remoteStorage.chunkExists(chunkPath);
        expect(exists).toBe(false);
      }
    }

    expect(newIndex.files.length).toBe(0);
  });
});
