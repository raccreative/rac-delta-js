import { describe, it, expect, beforeEach, afterAll } from 'vitest';
import { writeFile, mkdir, rm } from 'fs/promises';
import { join } from 'path';

import { HashWasmHasherService } from '../../src/infrastructure/services/hash-wasm-hasher-service';
import { MemoryValidationService } from '../../src/infrastructure/services/memory-validation-service';
import { FileEntry, RDIndex } from '../../src/core/models';

describe('MemoryValidationService', () => {
  let baseDir: string;
  let hasher: HashWasmHasherService;
  let service: MemoryValidationService;

  beforeEach(async () => {
    baseDir = join(__dirname, 'tmp-validation');
    await mkdir(baseDir, { recursive: true });

    hasher = new HashWasmHasherService();
    service = new MemoryValidationService(hasher);
  });

  afterAll(async () => {
    await rm(baseDir, { recursive: true, force: true });
  });

  it('returns true for valid file (size + hash)', async () => {
    const path = join(baseDir, 'file.txt');
    const content = Buffer.from('hello');
    await writeFile(path, content);

    const hash = await hasher.hashBuffer(content);

    const entry: FileEntry = {
      path: 'file.txt',
      hash,
      modifiedAt: Date.now(),
      size: content.length,
      chunks: [],
    };

    const result = await service.validateFile(entry, path);
    expect(result).toBe(true);
  });

  it('returns false if file does not exist', async () => {
    const entry: FileEntry = {
      path: 'nonexistent.txt',
      hash: 'fakehash',
      modifiedAt: Date.now(),
      size: 10,
      chunks: [],
    };

    const result = await service.validateFile(entry, join(baseDir, 'nonexistent.txt'));
    expect(result).toBe(false);
  });

  it('returns false if size is incorrect', async () => {
    const path = join(baseDir, 'wrong-size.txt');
    const content = Buffer.from('data');
    await writeFile(path, content);

    const hash = await hasher.hashBuffer(content);

    const entry: FileEntry = {
      path: 'wrong-size.txt',
      hash,
      modifiedAt: Date.now(),
      size: 999,
      chunks: [],
    };

    const result = await service.validateFile(entry, path);
    expect(result).toBe(false);
  });

  it('returns false if hash does not match', async () => {
    const path = join(baseDir, 'wrong-hash.txt');
    const content = Buffer.from('abc');
    await writeFile(path, content);

    const entry: FileEntry = {
      path: 'wrong-hash.txt',
      hash: 'invalidhash',
      modifiedAt: Date.now(),
      size: content.length,
      chunks: [],
    };

    const result = await service.validateFile(entry, path);
    expect(result).toBe(false);
  });

  it('returns true if all files in index are valid', async () => {
    const file1 = join(baseDir, 'a.txt');
    const file2 = join(baseDir, 'b.txt');
    const c1 = Buffer.from('x');
    const c2 = Buffer.from('y');

    await writeFile(file1, c1);
    await writeFile(file2, c2);

    const h1 = await hasher.hashBuffer(c1);
    const h2 = await hasher.hashBuffer(c2);

    const index: RDIndex = {
      version: 1,
      chunkSize: 10,
      createdAt: Date.now(),
      files: [
        { path: 'a.txt', hash: h1, modifiedAt: Date.now(), size: c1.length, chunks: [] },
        { path: 'b.txt', hash: h2, modifiedAt: Date.now(), size: c2.length, chunks: [] },
      ],
    };

    const result = await service.validateIndex(index, baseDir);
    expect(result).toBe(true);
  });

  it('returns false if any file in index is invalid', async () => {
    const file1 = join(baseDir, 'valid.txt');
    const file2 = join(baseDir, 'missing.txt');
    const c1 = Buffer.from('hello');
    await writeFile(file1, c1);
    const h1 = await hasher.hashBuffer(c1);

    const index: RDIndex = {
      version: 1,
      chunkSize: 10,
      createdAt: Date.now(),
      files: [
        { path: 'valid.txt', hash: h1, modifiedAt: Date.now(), size: c1.length, chunks: [] },
        { path: 'missing.txt', hash: 'fake', modifiedAt: Date.now(), size: 5, chunks: [] },
      ],
    };

    const result = await service.validateIndex(index, baseDir);
    expect(result).toBe(false);
  });
});
