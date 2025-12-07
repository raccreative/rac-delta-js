import { mkdir, open, rename, FileHandle, rm, access, stat } from 'fs/promises';
import { join, dirname, isAbsolute, resolve } from 'path';
import { Readable, PassThrough, Writable } from 'stream';
import { pipeline } from 'stream/promises';
import { createWriteStream } from 'fs';

import {
  ChunkSource,
  DEFAULT_IN_PLACE_RECONSTRUCTION_THRESHOLD,
  HasherService,
  ReconstructionOptions,
  ReconstructionService,
} from '../../core/services';
import { ChunkNotFoundException } from '../../core/exceptions';
import { DeltaPlan, FileEntry, Chunk } from '../../core/models';
import { streamToBuffer } from '../../core/utils';
import { Nullish } from '../../core/types';

export class MemoryReconstructionService implements ReconstructionService {
  constructor(private readonly hasher: HasherService) {}

  async reconstructAll(
    plan: DeltaPlan,
    outputDir: string,
    chunkSource: ChunkSource,
    options?: Nullish<ReconstructionOptions>
  ): Promise<void> {
    const dir = isAbsolute(outputDir) ? outputDir : resolve(process.cwd(), outputDir);

    await mkdir(dir, { recursive: true });

    const files = [...plan.newAndModifiedFiles];
    const total = files.length;
    let completed = 0;

    let globalBytesWritten = 0;
    let globalBytesReceived = 0;
    const startTime = Date.now();
    let error: Error | null = null;

    const queue = files.map((entry, index) => ({ entry, index }));

    const fileProgressMap = new Map<string, number>();

    const isNetworkSource = typeof chunkSource.streamChunks === 'function';
    const totalNetworkBytes = isNetworkSource
      ? plan.missingChunks.reduce((accumulator, chunk) => accumulator + chunk.size, 0)
      : 0;

    const totalBytesToWrite = files.reduce(
      (accumulator, file) => accumulator + (file.size ?? 0),
      0
    );

    const next = async (): Promise<void> => {
      while (queue.length && !error) {
        const { entry } = queue.shift()!;
        const outputPath = join(dir, entry.path);

        try {
          await this.reconstructFile(entry, outputPath, chunkSource, {
            ...options,
            onProgress: (fileProgress, fileBytesWritten, fileBytesReceived) => {
              fileProgressMap.set(entry.path, fileProgress);

              globalBytesWritten += fileBytesWritten ?? 0;

              if (isNetworkSource) {
                globalBytesReceived += fileBytesReceived ?? 0;
              }

              const reconstructProgress = Math.min(
                (globalBytesWritten / totalBytesToWrite) * 100,
                100
              );

              const networkProgress = isNetworkSource
                ? Math.min((globalBytesReceived / totalNetworkBytes) * 100, 100)
                : undefined;

              const elapsed = Math.max((Date.now() - startTime) / 1000, 0.001);
              const diskSpeed = globalBytesWritten / elapsed;
              const netSpeed = isNetworkSource ? globalBytesReceived / elapsed : 0;

              if (isNetworkSource) {
                options?.onProgress?.(reconstructProgress, diskSpeed, networkProgress, netSpeed);
              } else {
                options?.onProgress?.(reconstructProgress, diskSpeed);
              }
            },
          });

          completed++;
          fileProgressMap.set(entry.path, 100);
        } catch (err) {
          error = err instanceof Error ? err : new Error(String(err));
          break;
        }
      }
    };

    const workers = Array.from({ length: Math.min(options?.fileConcurrency ?? 5, total) }, next);
    await Promise.allSettled(workers);

    if (error) {
      throw error;
    }
  }

  async reconstructFile(
    entry: FileEntry,
    outputPath: string,
    chunkSource: ChunkSource,
    options: ReconstructionOptions = {
      forceRebuild: false,
      verifyAfterRebuild: true,
      inPlaceReconstructionThreshold: DEFAULT_IN_PLACE_RECONSTRUCTION_THRESHOLD,
    }
  ): Promise<void> {
    const defOutputPath = isAbsolute(outputPath) ? outputPath : resolve(process.cwd(), outputPath);

    const { forceRebuild, verifyAfterRebuild, inPlaceReconstructionThreshold } = options;
    const tempPath = `${defOutputPath}.tmp`;

    await mkdir(dirname(defOutputPath), { recursive: true });

    const exists = await this.fileExists(defOutputPath);
    if (exists && !forceRebuild) {
      const matches = await this.hasher.verifyFile(defOutputPath, entry.hash);
      if (matches) {
        return;
      }
    }

    const stats = exists ? await stat(defOutputPath) : null;
    const isLargeFile = stats && stats.size > (inPlaceReconstructionThreshold as number);

    const existingLargeFileWithoutRebuild = exists && !forceRebuild && isLargeFile;

    const progressCb = (chunkBytes: number, netBytes: number, processedChunks: number) => {
      const fileProgress = (processedChunks / entry.chunks.length) * 100;
      options.onProgress?.(fileProgress, chunkBytes, netBytes);
    };

    try {
      if (existingLargeFileWithoutRebuild && inPlaceReconstructionThreshold !== 0) {
        await this.reconstructInPlace(entry, defOutputPath, chunkSource, progressCb);
      } else {
        await this.reconstructToTemp(
          entry,
          defOutputPath,
          tempPath,
          chunkSource,
          !!verifyAfterRebuild,
          exists,
          !!options.forceRebuild,
          progressCb
        );
      }
    } catch (err) {
      await rm(tempPath, { force: true });
      throw err;
    }
  }

  // This will reconstruct to stream, not to disk
  async reconstructToStream(entry: FileEntry, chunkSource: ChunkSource): Promise<Readable> {
    const output = new PassThrough({ highWaterMark: 1024 * 1024 });
    const chunks = entry.chunks ?? [];

    // Starts async reconstruction without blocking stream return
    const processChunks = async () => {
      try {
        for await (const { data } of this.fetchChunksSmart(chunks, chunkSource)) {
          await this.writeToStream(data, output);
        }

        output.end();
      } catch (err) {
        output.destroy(err instanceof Error ? err : new Error(String(err)));
      }
    };

    void processChunks();

    return output;
  }

  private async fileExists(path: string): Promise<boolean> {
    try {
      await access(path);
      return true;
    } catch {
      return false;
    }
  }

  /** Performs partial reconstruction directly in the existing file */
  private async reconstructInPlace(
    entry: FileEntry,
    outputPath: string,
    chunkSource: ChunkSource,
    progressCb?: (chunkBytes: number, netBytes: number, processedChunks: number) => void
  ): Promise<void> {
    const fd = await open(outputPath, 'r+');

    const isNetworkSource = typeof chunkSource.streamChunks === 'function';

    try {
      const chunkMap = new Map(entry.chunks.map((c) => [c.hash, c]));
      let processed = 0;

      for await (const { hash, data } of this.fetchChunksSmart(entry.chunks, chunkSource, false)) {
        const chunk = chunkMap.get(hash);
        if (!chunk) {
          continue;
        }

        const buffer = Buffer.isBuffer(data) ? data : await streamToBuffer(data);

        await fd.write(buffer, 0, buffer.length, chunk.offset);

        processed++;
        const netBytes = isNetworkSource ? buffer.length : 0;
        progressCb?.(buffer.length, netBytes, processed);
      }
    } finally {
      await fd.close();
    }
  }

  /** Reconstructs file fully or partially via .tmp file and replaces it atomically */
  private async reconstructToTemp(
    entry: FileEntry,
    outputPath: string,
    tempPath: string,
    chunkSource: ChunkSource,
    verifyAfterRebuild: boolean,
    fileExists: boolean,
    force: boolean,
    progressCb?: (chunkBytes: number, netBytes: number, processedChunks: number) => void
  ): Promise<void> {
    const writeStream = createWriteStream(tempPath, { flags: 'w' });

    let writeError: Error | null = null;
    const onWriteError = (err: Error) => {
      writeError = err;
    };

    writeStream.once('error', onWriteError);

    let processed = 0;
    const isNetworkSource = typeof chunkSource.streamChunks === 'function';

    try {
      // Partial reconstruction via reading from existing file
      if (fileExists && !force) {
        const readFd = await open(outputPath, 'r');
        try {
          for (const chunk of entry.chunks) {
            await this.processChunkDataSmart(chunk, readFd, chunkSource, writeStream);

            processed++;
            progressCb?.(chunk.size, 0, processed);
          }
        } finally {
          await readFd.close();
        }
      }

      // Full reconstruction
      if (!fileExists || force) {
        try {
          for await (const { data } of this.fetchChunksSmart(entry.chunks, chunkSource)) {
            let totalWritten = 0;

            await this.writeToStream(data, writeStream, (totalBytes) => {
              totalWritten = totalBytes;
            });

            processed++;
            progressCb?.(totalWritten, isNetworkSource ? totalWritten : 0, processed);
          }

          if (writeError) {
            throw writeError;
          }
        } catch (err) {
          writeStream.destroy();
          throw err;
        } finally {
          writeStream.removeListener('error', onWriteError);
          writeStream.end();
        }
      }

      writeStream.end();
      await new Promise<void>((resolve, reject) => {
        writeStream.on('finish', resolve);
        writeStream.on('error', reject);
      });

      if (verifyAfterRebuild) {
        const valid = await this.hasher.verifyFile(tempPath, entry.hash);
        if (!valid) {
          throw new Error(`Hash mismatch after reconstructing ${entry.path}`);
        }
      }

      await rename(tempPath, outputPath);
    } catch (err) {
      throw err;
    }
  }

  /**
   * Tries to read an existing chunk from a file descriptor if possible.
   * Falls back to fetching from chunkSource if hash does not match.
   */
  private async processChunkDataSmart(
    chunk: Chunk,
    fd: FileHandle,
    chunkSource?: Nullish<ChunkSource>,
    writeStream?: Nullish<Writable>
  ): Promise<void> {
    const buffer = Buffer.alloc(chunk.size);
    await fd.read(buffer, 0, chunk.size, chunk.offset);

    if (await this.hasher.verifyChunk(buffer, chunk.hash)) {
      await this.writeToStream(buffer, writeStream);
      return;
    }

    // fallback
    const data = await this.getChunkData(chunk, chunkSource);

    if (Buffer.isBuffer(data)) {
      await this.writeToStream(data, writeStream);
      return;
    }

    if (writeStream) {
      await pipeline(data, writeStream, { end: false });
    }
  }

  private async getChunkData(chunk: Chunk, chunkSource?: Nullish<ChunkSource>): Promise<Buffer> {
    if (!chunkSource) {
      throw new Error(`ChunkSource not provided for chunk ${chunk.hash}`);
    }

    const data = await chunkSource.getChunk(chunk.hash);

    if (!data) {
      throw new ChunkNotFoundException(`${chunk.hash} not found in storage`);
    }

    return data;
  }

  private async writeToStream(
    data: Buffer | Readable,
    stream?: Nullish<Writable>,
    onFinish?: (totalBytes: number) => void
  ) {
    if (!stream) {
      return;
    }

    if (Buffer.isBuffer(data)) {
      const canContinue = stream.write(data);
      if (!canContinue) {
        await new Promise<void>((resolve) => stream.once('drain', resolve));
      }

      onFinish?.(data.length);
      return;
    }

    let totalBytes = 0;

    await new Promise<void>((resolve, reject) => {
      const onError = (err: unknown) => {
        cleanup();
        reject(err instanceof Error ? err : new Error(String(err)));
      };

      const onEnd = () => {
        cleanup();
        onFinish?.(totalBytes);
        resolve();
      };

      const cleanup = () => {
        data.off('error', onError);
        data.off('end', onEnd);
        stream.off('error', onError);
      };

      data.on('error', onError);
      stream.on('error', onError);
      data.on('end', onEnd);

      data.on('data', (chunk) => {
        totalBytes += chunk.length;

        const canContinue = stream.write(chunk);
        if (!canContinue) {
          data.pause();
          stream.once('drain', () => data.resume());
        }
      });
    });
  }

  private async *fetchChunksSmart(
    chunks: Chunk[],
    chunkSource: ChunkSource,
    preserveOrder = true
  ): AsyncGenerator<{ hash: string; data: Buffer | Readable }> {
    const hashes = chunks.map((c) => c.hash);

    // 1. streamChunks available: streaming
    if (chunkSource.streamChunks) {
      for await (const { hash, data } of chunkSource.streamChunks(hashes, { preserveOrder })) {
        yield { hash: hash, data };
      }
      return;
    }

    // 2. getChunks available: download all chunks
    if (chunkSource.getChunks) {
      const map = await chunkSource.getChunks(hashes);

      for (const hash of hashes) {
        const data = map.get(hash);
        if (data) {
          yield { hash, data };
        }
      }

      return;
    }

    // 3. fallback: individual getChunk
    for (const chunk of chunks) {
      const data = await chunkSource.getChunk(chunk.hash);
      yield { hash: chunk.hash, data };
    }
  }
}
