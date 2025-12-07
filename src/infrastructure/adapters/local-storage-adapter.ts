import { mkdir, access, rm, readdir, stat, readFile, writeFile } from 'fs/promises';
import { createReadStream, createWriteStream } from 'fs';
import { Readable } from 'stream';
import { join, dirname } from 'path';

import { ChunkAlreadyExistsException } from '../../core/exceptions';
import { BlobInfo, HashStorageAdapter } from '../../core/adapters';
import { LocalStorageConfig } from '../../core/config';
import { RDIndex } from '../../core/models';
import { Nullish } from '../../core/types';

export class LocalStorageAdapter extends HashStorageAdapter {
  constructor(private readonly config: LocalStorageConfig) {
    super();
  }

  async dispose(): Promise<void> {
    return;
  }

  private resolveChunkPath(hash: string) {
    const prefix = this.config.pathPrefix ?? '';
    return join(this.config.basePath, prefix, 'chunks', hash);
  }

  private resolveIndexPath() {
    const prefix = this.config.pathPrefix ?? '';
    return join(this.config.basePath, prefix, 'rd-index.json');
  }

  async getChunk(hash: string): Promise<Readable | null> {
    const path = this.resolveChunkPath(hash);

    try {
      await access(path);
      return createReadStream(path);
    } catch {
      return null;
    }
  }

  async putChunk(
    hash: string,
    data: Readable,
    opts?: Nullish<{ overwrite?: Nullish<boolean> }>
  ): Promise<void> {
    const path = this.resolveChunkPath(hash);
    await mkdir(dirname(path), { recursive: true });

    if (!opts?.overwrite) {
      try {
        await access(path);
        throw new ChunkAlreadyExistsException(`${hash}`);
      } catch (error) {
        const nodeError = error as NodeJS.ErrnoException;
        if (nodeError.code !== 'ENOENT') {
          throw error;
        }
      }
    }

    await new Promise<void>((resolve, reject) => {
      const writeStream = createWriteStream(path);

      data.pipe(writeStream);

      writeStream.on('finish', () => resolve());
      writeStream.on('error', (err) => reject(err));

      data.on('error', (err) => reject(err));
    });
  }

  async chunkExists(hash: string): Promise<boolean> {
    const path = this.resolveChunkPath(hash);

    try {
      await access(path);
      return true;
    } catch {
      return false;
    }
  }

  async deleteChunk(hash: string): Promise<void> {
    const path = this.resolveChunkPath(hash);
    await rm(path, { force: true });
  }

  async listChunks(): Promise<string[]> {
    const prefix = this.config.pathPrefix ?? '';
    const chunksDir = join(this.config.basePath, prefix, 'chunks');

    const hashes: string[] = [];

    try {
      const entries = await readdir(chunksDir, { withFileTypes: true });
      for (const entry of entries) {
        if (entry.isFile()) {
          hashes.push(entry.name);
        }
      }
    } catch {
      // if no /chunks directory return empty
    }

    return hashes;
  }

  async getChunkInfo(hash: string): Promise<BlobInfo | null> {
    const path = this.resolveChunkPath(hash);

    try {
      const stats = await stat(path);
      return {
        hash,
        size: stats.size,
        modified: stats.mtime,
      };
    } catch {
      return null;
    }
  }

  async getRemoteIndex(): Promise<RDIndex | null> {
    const indexPath = this.resolveIndexPath();

    try {
      const data = await readFile(indexPath, 'utf-8');
      return JSON.parse(data) as RDIndex;
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException;
      if (nodeError.code === 'ENOENT') {
        return null;
      }

      throw error;
    }
  }

  async putRemoteIndex(index: RDIndex): Promise<void> {
    const indexPath = this.resolveIndexPath();
    await mkdir(dirname(indexPath), { recursive: true });
    await writeFile(indexPath, JSON.stringify(index, null, 2), 'utf-8');
  }
}
