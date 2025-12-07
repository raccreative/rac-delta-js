import { join, isAbsolute, resolve } from 'path';
import { open } from 'fs/promises';
import { Readable } from 'stream';

import { UploadOptions, HashUploadPipeline } from '../../core/pipelines';
import { ChunkEntry, DeltaPlan, RDIndex } from '../../core/models';
import { HashStorageAdapter } from '../../core/adapters';
import { RacDeltaConfig } from '../../core/config';
import { DeltaService } from '../../core/services';
import { Nullish } from '../../core/types';

export class DefaultHashUploadPipeline extends HashUploadPipeline {
  constructor(
    protected readonly storage: HashStorageAdapter,
    protected readonly delta: DeltaService,
    protected readonly config: RacDeltaConfig
  ) {
    super(storage, delta, config);
  }

  async execute(
    directory: string,
    remoteIndex?: Nullish<RDIndex>,
    options?: Nullish<UploadOptions>
  ): Promise<RDIndex> {
    try {
      this.changeState('scanning', options);
      const localIndex = await this.scanDirectory(directory, options?.ignorePatterns);

      let remoteIndexToUse: RDIndex | null = remoteIndex ?? null;

      // If no remote index provided and no force upload, we try to get remote index using storageAdapter
      if (!remoteIndex && !options?.force) {
        const result = await this.storage.getRemoteIndex();

        if (!result && options?.requireRemoteIndex) {
          throw new Error('Remote rd-index.json could not be found');
        }

        remoteIndexToUse = result;
      }

      // If force we always upload everything, so we just ignore remote index
      if (options?.force) {
        remoteIndexToUse = null;
      }

      // If no remoteIndex provided, everything will be uploaded
      this.changeState('comparing', options);
      const deltaPlan = await this.delta.compareForUpload(localIndex, remoteIndexToUse);

      if (
        !options?.force &&
        !deltaPlan.missingChunks.length &&
        !deltaPlan.obsoleteChunks.length &&
        !deltaPlan.deletedFiles.length
      ) {
        console.info('No changes to upload or delete, remote is up to date.');

        return localIndex;
      }

      if (deltaPlan.missingChunks.length) {
        this.changeState('uploading', options);
        await this.uploadMissingChunks(deltaPlan, directory, !!options?.force, options);
      }

      if (remoteIndexToUse && deltaPlan.obsoleteChunks.length) {
        this.changeState('cleaning', options);
        await this.deleteObsoleteChunks(deltaPlan, options);
      }

      this.changeState('finalizing', options);
      await this.uploadIndex(localIndex);

      return localIndex;
    } finally {
      await this.storage.dispose();
    }
  }

  async scanDirectory(localDir: string, ignorePatterns?: Nullish<string[]>): Promise<RDIndex> {
    const localIndex = await this.delta.createIndexFromDirectory(
      localDir,
      this.config.chunkSize,
      this.config.maxConcurrency,
      ignorePatterns
    );
    return localIndex;
  }

  async uploadMissingChunks(
    plan: DeltaPlan,
    baseDir: string,
    force: boolean,
    options?: Nullish<UploadOptions>
  ): Promise<void> {
    if (!plan.missingChunks?.length) {
      return;
    }

    const dir = isAbsolute(baseDir) ? baseDir : resolve(process.cwd(), baseDir);

    const concurrency = this.config.maxConcurrency ?? 5;
    const files = this.groupChunksByFile(plan.missingChunks);
    const totalChunks = plan.missingChunks.length;

    let uploadedChunks = 0;
    let uploadedBytes = 0;
    const startTime = Date.now();

    const queue = [...files];

    const worker = async () => {
      while (true) {
        const next = queue.pop();
        if (!next) {
          break;
        }

        const [relativePath, chunks] = next;
        chunks.sort((a, b) => a.offset - b.offset);

        const filePath = join(dir, relativePath);
        const fileHandle = await open(filePath, 'r');

        try {
          const buffer = Buffer.alloc(this.config.chunkSize);

          for (const { offset, size, hash } of chunks) {
            const { bytesRead } = await fileHandle.read(buffer, 0, size, offset);

            const data = buffer.subarray(0, bytesRead);
            const stream = Readable.from(data);

            await this.storage.putChunk(hash, stream, {
              overwrite: true,
              size,
            });

            uploadedChunks++;
            uploadedBytes += bytesRead;

            const percent = ((uploadedChunks / totalChunks) * 100).toFixed(1);
            const elapsed = (Date.now() - startTime) / 1000; // seconds
            const speed = uploadedBytes / elapsed;

            this.updateProgress(Number(percent), 'upload', speed, options);
          }
        } finally {
          await fileHandle.close();
        }
      }
    };

    await Promise.all(Array.from({ length: concurrency }, () => worker()));
  }

  async uploadIndex(index: RDIndex): Promise<void> {
    await this.storage.putRemoteIndex(index);
  }

  async deleteObsoleteChunks(plan: DeltaPlan, options?: Nullish<UploadOptions>): Promise<void> {
    if (!plan.obsoleteChunks?.length) {
      return;
    }

    const concurrency = this.config.maxConcurrency ?? 5;
    const queue = [...plan.obsoleteChunks];
    const total = queue.length;
    let deleted = 0;

    if (total === 0) {
      return;
    }

    const failed: string[] = [];
    const maxRetries = 3;

    const worker = async () => {
      while (true) {
        const chunk = queue.pop();

        if (!chunk) {
          break;
        }

        let success = false;

        for (let attempt = 1; attempt <= maxRetries && !success; attempt++) {
          try {
            await this.storage.deleteChunk(chunk.hash);
            success = true;
          } catch (err) {
            if (attempt === maxRetries) {
              failed.push(chunk.hash);
            } else {
              await new Promise((res) => setTimeout(res, 100 * attempt));
            }
          }
        }

        deleted++;
        const percent = (deleted / total) * 100;
        this.updateProgress(percent, 'deleting', undefined, options);
      }
    };

    await Promise.all(Array.from({ length: concurrency }, () => worker()));

    if (failed.length > 0) {
      throw new Error(`Failed to delete ${failed.length}/${total} chunks: ${failed.join(', ')}`);
    }
  }

  private groupChunksByFile(chunks: ChunkEntry[]) {
    const groups = new Map<string, ChunkEntry[]>();

    for (const chunk of chunks) {
      if (!groups.has(chunk.filePath)) {
        groups.set(chunk.filePath, []);
      }

      groups.get(chunk.filePath)!.push(chunk);
    }

    return Array.from(groups.entries());
  }
}
