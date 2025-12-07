import { stat, readFile, access, mkdir, writeFile, rm } from 'fs/promises';
import { pipeline } from 'stream/promises';
import { createWriteStream } from 'fs';
import { join, dirname } from 'path';
import { Readable } from 'stream';

import { ChunkNotFoundException } from '../../core/exceptions';
import { ChunkSource } from '../../core/services';

export class DiskChunkSource implements ChunkSource {
  private cacheDir: string;

  constructor(cacheDir: string) {
    this.cacheDir = cacheDir;
  }

  async getChunk(hash: string): Promise<Buffer> {
    const filePath = join(this.cacheDir, hash);

    try {
      const stats = await stat(filePath);
      if (!stats.isFile()) {
        throw new Error(`Resource is not a file`);
      }
      return readFile(filePath);
    } catch {
      throw new ChunkNotFoundException(`${hash} not found on disk`);
    }
  }

  async getChunks(hashes: string[]): Promise<Map<string, Buffer>> {
    const results = new Map<string, Buffer>();

    for (const hash of hashes) {
      const buffer = await this.getChunk(hash);
      results.set(hash, buffer);
    }

    return results;
  }

  async hasChunk(hash: string): Promise<boolean> {
    const filePath = join(this.cacheDir, hash);
    try {
      await access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  async setChunk(hash: string, data: Buffer | Readable): Promise<void> {
    const filePath = join(this.cacheDir, hash);
    await mkdir(dirname(filePath), { recursive: true });

    if (data instanceof Readable) {
      const writeStream = createWriteStream(filePath);
      await pipeline(data, writeStream);
    } else {
      await writeFile(filePath, data);
    }
  }

  async clear(): Promise<void> {
    await rm(this.cacheDir, { recursive: true, force: true });
  }
}
