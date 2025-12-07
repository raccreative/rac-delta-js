import { writeFile, rm, readdir, readFile } from 'fs/promises';
import { join, resolve, isAbsolute } from 'path';

import { DeltaService, ReconstructionService, ValidationService } from '../../core/services';
import { DownloadOptions, HashDownloadPipeline, UpdateStrategy } from '../../core/pipelines';
import { ChunkSource } from '../../core/services/reconstruction-service';
import { ChunkEntry, DeltaPlan, RDIndex } from '../../core/models';
import { streamToBuffer } from '../../core/utils/stream-to-buffer';
import { HashStorageAdapter } from '../../core/adapters';
import { RacDeltaConfig } from '../../core/config';
import { Nullish } from '../../core/types';

import { StorageChunkSource } from '../chunk-sources/storage-chunk-source';
import { MemoryChunkSource } from '../chunk-sources/memory-chunk-source';
import { DiskChunkSource } from '../chunk-sources/disk-chunk-source';

export class DefaultHashDownloadPipeline extends HashDownloadPipeline {
  constructor(
    protected readonly reconstruction: ReconstructionService,
    protected readonly validation: ValidationService,
    protected readonly storage: HashStorageAdapter,
    protected readonly config: RacDeltaConfig,
    protected readonly delta: DeltaService
  ) {
    super(storage, delta, reconstruction, validation, config);
  }

  async execute(
    localDir: string,
    strategy: UpdateStrategy,
    remoteIndex?: Nullish<RDIndex>,
    options?: Nullish<DownloadOptions>
  ): Promise<void> {
    try {
      this.changeState('scanning', options);
      const localIndex = !options?.force
        ? options?.useExistingIndex
          ? ((await this.findLocalIndex(localDir)) ?? (await this.loadLocalIndex(localDir)))
          : await this.loadLocalIndex(localDir)
        : null;

      const remoteIndexToUse = remoteIndex ?? (await this.storage.getRemoteIndex());

      if (!remoteIndexToUse) {
        throw new Error(
          'Remote rd-index not provided and was not found in storage. Check if storage pathPrefix is correct or provide an index manually'
        );
      }

      const plan = await this.delta.compareForDownload(localIndex, remoteIndexToUse);

      if (
        !options?.force &&
        !plan.missingChunks.length &&
        !plan.obsoleteChunks.length &&
        !plan.deletedFiles.length
      ) {
        console.info('No changes to download, you are up to date.');
        return;
      }

      let chunkSource: ChunkSource | null = null;

      if (strategy === UpdateStrategy.DownloadAllFirstToMemory) {
        this.changeState('downloading', options);
        chunkSource = await this.downloadAllMissingChunks(plan, 'memory', options);
      }

      // StorageChunkSource admits streaming, so it will be downloading chunks while reconstructing in service
      if (strategy === UpdateStrategy.StreamFromNetwork) {
        chunkSource = new StorageChunkSource(this.storage);
      }

      if (strategy === UpdateStrategy.DownloadAllFirstToDisk) {
        this.changeState('downloading', options);
        chunkSource = await this.downloadAllMissingChunks(plan, 'disk', options);
      }

      if (!chunkSource) {
        throw new Error('No chunkSource found');
      }

      if (plan.newAndModifiedFiles.length) {
        this.changeState('reconstructing', options);
        await this.reconstruction.reconstructAll(plan, localDir, chunkSource, {
          forceRebuild: options?.force,
          verifyAfterRebuild: true,
          fileConcurrency: options?.fileReconstructionConcurrency,
          inPlaceReconstructionThreshold: options?.inPlaceReconstructionThreshold,
          onProgress: (reconstructProgress, diskSpeed, networkProgress, networkSpeed) => {
            this.updateProgress(
              reconstructProgress,
              'reconstructing',
              diskSpeed,
              undefined,
              options
            );

            if (networkProgress) {
              this.updateProgress(networkProgress, 'download', 0, networkSpeed, options);
            }
          },
        });
      }

      if (plan.obsoleteChunks.length || plan.deletedFiles.length) {
        this.changeState('cleaning', options);
        await this.verifyAndDeleteObsoleteChunks(
          plan,
          localDir,
          remoteIndexToUse,
          chunkSource,
          options
        );
      }

      await this.saveLocalIndex(localDir, remoteIndexToUse);

      if (chunkSource instanceof DiskChunkSource) {
        chunkSource.clear();
      }
    } finally {
      await this.storage.dispose();
    }
  }

  async loadLocalIndex(localDir: string): Promise<RDIndex> {
    const localIndex = await this.delta.createIndexFromDirectory(
      localDir,
      this.config.chunkSize,
      this.config.maxConcurrency
    );
    return localIndex;
  }

  async downloadAllMissingChunks(
    plan: DeltaPlan,
    target: 'memory' | 'disk',
    options?: Nullish<DownloadOptions>
  ): Promise<ChunkSource> {
    if (target === 'disk' && !options?.chunksSavePath) {
      throw new Error('Error: chunksSavePath must be provided under options');
    }

    let chunksSavePath = undefined;
    if (target === 'disk' && options?.chunksSavePath) {
      chunksSavePath = isAbsolute(options?.chunksSavePath)
        ? options?.chunksSavePath
        : resolve(process.cwd(), options?.chunksSavePath);
    }

    const chunkSource =
      target === 'memory' ? new MemoryChunkSource() : new DiskChunkSource(chunksSavePath as string);

    const chunks = plan.missingChunks;
    let completed = 0;
    let totalBytes = 0;

    const concurrency = this.config.maxConcurrency ?? 6;
    const queue = [...chunks];

    let lastUpdateTime = Date.now();
    let lastBytes = 0;
    let speed = 0;

    const worker = async () => {
      while (queue.length) {
        const chunk = queue.pop()!;
        const readable = await this.storage.getChunk(chunk.hash);
        if (!readable) {
          throw new Error(`Chunk missing: ${chunk.hash}`);
        }

        const buffer = await streamToBuffer(readable);
        chunkSource.setChunk(chunk.hash, buffer);

        completed++;
        totalBytes += buffer.length;

        const now = Date.now();
        const elapsed = now - lastUpdateTime;

        // each 100ms
        if (elapsed >= 100) {
          const bytesDiff = totalBytes - lastBytes;
          speed = bytesDiff / (elapsed / 1000);

          lastUpdateTime = now;
          lastBytes = totalBytes;

          const percent = ((completed / chunks.length) * 100).toFixed(1);
          this.updateProgress(Number(percent), 'download', 0, speed, options);
        }
      }
    };

    await Promise.all(Array.from({ length: concurrency }, () => worker()));

    const percent = 100;
    const totalTime = (Date.now() - lastUpdateTime) / 1000;
    const avgSpeed = totalBytes / (totalTime || 1);
    this.updateProgress(percent, 'download', 0, avgSpeed, options);

    return chunkSource;
  }

  async verifyAndDeleteObsoleteChunks(
    plan: DeltaPlan,
    localDir: string,
    remoteIndex: RDIndex,
    chunkSource: ChunkSource,
    options?: Nullish<DownloadOptions>
  ) {
    const dir = isAbsolute(localDir) ? localDir : resolve(process.cwd(), localDir);

    const obsoleteByFile = this.groupByFile(plan.obsoleteChunks);

    const deletedFiles: string[] = [];
    const verifiedFiles: string[] = [];
    const rebuiltFiles: string[] = [];

    const allFiles = new Set<string>([...plan.deletedFiles, ...obsoleteByFile.keys()]);
    const totalFiles = allFiles.size;
    let completedFiles = 0;

    for (const filePath of allFiles) {
      const absPath = join(dir, filePath);
      const remoteFile = remoteIndex.files.find((file) => file.path === filePath);

      // Fully removed file
      if (!remoteFile || plan.deletedFiles.includes(filePath)) {
        await rm(absPath, { force: true });
        deletedFiles.push(filePath);
      } else {
        const isValid = await this.validation.validateFile(remoteFile, absPath);

        if (!isValid) {
          await this.reconstruction.reconstructFile(remoteFile, absPath, chunkSource);
          rebuiltFiles.push(filePath);
        } else {
          verifiedFiles.push(filePath);
        }
      }

      completedFiles++;
      this.updateProgress(
        (completedFiles / totalFiles) * 100,
        'deleting',
        undefined,
        undefined,
        options
      );
    }

    return { deletedFiles, verifiedFiles, rebuiltFiles };
  }

  async saveLocalIndex(localDir: string, index: RDIndex) {
    const dir = isAbsolute(localDir) ? localDir : resolve(process.cwd(), localDir);

    const indexPath = join(dir, 'rd-index.json');
    await writeFile(indexPath, JSON.stringify(index, null, 2), 'utf-8');
  }

  async findLocalIndex(localDir: string): Promise<RDIndex | null> {
    const dir = isAbsolute(localDir) ? localDir : resolve(process.cwd(), localDir);

    const files = await readdir(dir);
    const indexFile = files.find((f) => f === 'rd-index.json');

    if (!indexFile) {
      return null;
    }

    const filePath = join(localDir, indexFile);
    const data = await readFile(filePath, 'utf-8');

    try {
      const parsed: RDIndex = JSON.parse(data);
      return parsed;
    } catch (error) {
      if (error instanceof Error) {
        throw new Error(`Invalid RDIndex JSON at ${filePath}: ${error.message}`);
      }

      throw error;
    }
  }

  private groupByFile(chunks: ChunkEntry[]): Map<string, ChunkEntry[]> {
    const map = new Map<string, ChunkEntry[]>();

    for (const chunk of chunks) {
      if (!map.has(chunk.filePath)) {
        map.set(chunk.filePath, []);
      }

      map.get(chunk.filePath)!.push(chunk);
    }

    return map;
  }
}
