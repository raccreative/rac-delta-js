import { isAbsolute, resolve, join } from 'path';
import { createBLAKE3 } from 'hash-wasm';
import { createReadStream } from 'fs';
import { stat } from 'fs/promises';

import { HasherService, StreamingHasher } from '../../core/services';
import { invariant } from '../../core/utils/invariant';
import { AsyncChunkStream } from '../../core/services';
import { FileEntry, Chunk } from '../../core/models';
import { Nullish } from '../../core/types';

export class HashWasmHasherService implements HasherService {
  async hashFile(filePath: string, rootDir: string, chunkSize: number): Promise<FileEntry> {
    invariant('path must be a valid string', typeof filePath === 'string' && filePath !== '');
    invariant('chunkSize must be a valid number > 0', chunkSize > 0);

    const dir = isAbsolute(rootDir) ? rootDir : resolve(process.cwd(), rootDir);
    const finalFilePath = join(dir, filePath);

    try {
      const stats = await stat(finalFilePath);
      const fileHasher = await createBLAKE3();
      const chunks: Chunk[] = [];

      let offset = 0;
      const stream = createReadStream(finalFilePath, { highWaterMark: chunkSize });

      for await (const chunk of stream) {
        const chunkHasher = await createBLAKE3();
        chunkHasher.update(chunk);
        const chunkHash = chunkHasher.digest('hex');

        fileHasher.update(chunk);

        chunks.push({
          hash: chunkHash,
          offset,
          size: chunk.length,
        });

        offset += chunk.length;
      }

      const fileHash = fileHasher.digest('hex');

      return {
        path: filePath,
        size: stats.size,
        modifiedAt: stats.mtimeMs,
        hash: fileHash,
        chunks,
      };
    } catch (err: unknown) {
      if (err instanceof Error) {
        throw new Error(`HasherService.hashFile failed: ${err.message}`);
      }

      throw err;
    }
  }

  async hashStream(
    stream: AsyncChunkStream,
    onChunk?: Nullish<(chunk: Uint8Array) => void>
  ): Promise<Chunk[]> {
    const chunks: Chunk[] = [];
    let offset = 0;

    try {
      for await (const chunk of stream) {
        const chunkHasher = await createBLAKE3();
        chunkHasher.update(chunk);
        const chunkHash = chunkHasher.digest('hex');

        if (onChunk) {
          onChunk(chunk);
        }

        chunks.push({
          hash: chunkHash,
          offset,
          size: chunk.length,
        });

        offset += chunk.length;
      }

      if (stream.close) {
        await stream.close();
      }

      return chunks;
    } catch (err: unknown) {
      if (err instanceof Error) {
        throw new Error(`HasherService.hashStream failed: ${err.message}`);
      }

      throw err;
    }
  }

  async verifyChunk(data: Uint8Array, expectedHash: string): Promise<boolean> {
    const hasher = await createBLAKE3();
    hasher.update(data);
    const actualHash = hasher.digest('hex');

    return actualHash === expectedHash;
  }

  async hashBuffer(data: Uint8Array): Promise<string> {
    const hasher = await createBLAKE3();
    hasher.update(data);
    return hasher.digest('hex');
  }

  async verifyFile(path: string, expectedHash: string): Promise<boolean> {
    invariant('path must be a valid string', typeof path === 'string' && path !== '');

    const finalPath = isAbsolute(path) ? path : resolve(process.cwd(), path);

    const hasher = await createBLAKE3();
    const stream = createReadStream(finalPath);

    for await (const chunk of stream) {
      hasher.update(chunk);
    }

    const actualHash = hasher.digest('hex');
    return actualHash === expectedHash;
  }

  async createStreamingHasher(): Promise<StreamingHasher> {
    const native = await createBLAKE3();

    return {
      update(data: Uint8Array | Buffer) {
        native.update(data);
      },
      digest(encoding: 'hex' = 'hex') {
        return native.digest(encoding);
      },
    };
  }
}
