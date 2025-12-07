import { writeFile, rm, readdir, readFile } from 'fs/promises';
import { join, resolve, isAbsolute } from 'path';

import { UrlDownloadPipeline, DownloadOptions, UpdateStrategy } from '../../core/pipelines';
import { ChunkEntry, ChunkUrlInfo, DeltaPlan, RDIndex } from '../../core/models';
import { DeltaService, ReconstructionService, ValidationService } from '../../core/services';
import { ChunkSource } from '../../core/services/reconstruction-service';
import { streamToBuffer } from '../../core/utils/stream-to-buffer';
import { UrlStorageAdapter } from '../../core/adapters';
import { RacDeltaConfig } from '../../core/config';
import { Nullish } from '../../core/types';

import { StorageChunkSource } from '../chunk-sources/storage-chunk-source';
import { MemoryChunkSource } from '../chunk-sources/memory-chunk-source';
import { DiskChunkSource } from '../chunk-sources/disk-chunk-source';

export class DefaultUrlDownloadPipeline extends UrlDownloadPipeline {
  constructor(
    protected readonly storage: UrlStorageAdapter,
    protected readonly reconstruction: ReconstructionService,
    protected readonly validation: ValidationService,
    protected readonly delta: DeltaService,
    protected readonly config: RacDeltaConfig
  ) {
    super(storage, reconstruction, validation, delta, config);
  }

  async execute(
    localDir: string,
    urls: {
      downloadUrls: Record<string, ChunkUrlInfo>;
      indexUrl: string;
    },
    strategy: UpdateStrategy,
    plan?: Nullish<DeltaPlan>,
    options?: Nullish<DownloadOptions>
  ): Promise<void> {
    this.changeState('scanning', options);

    const remoteIndex = await this.storage.getRemoteIndexByUrl(urls.indexUrl);
    if (!remoteIndex) {
      throw new Error(`No remote rd-index found with the url: ${urls.indexUrl}`);
    }

    const localIndex = plan
      ? null
      : options?.useExistingIndex
        ? await this.findLocalIndex(localDir)
        : await this.loadLocalIndex(localDir);

    const planToUse = plan ? plan : await this.delta.compareForDownload(localIndex, remoteIndex);

    let chunkSource: ChunkSource | null = null;

    if (strategy === UpdateStrategy.DownloadAllFirstToMemory) {
      this.changeState('downloading', options);
      chunkSource = await this.downloadAllMissingChunks(urls.downloadUrls, 'memory', options);
    }

    if (strategy === UpdateStrategy.DownloadAllFirstToDisk) {
      this.changeState('downloading', options);
      chunkSource = await this.downloadAllMissingChunks(urls.downloadUrls, 'disk', options);
    }

    if (strategy === UpdateStrategy.StreamFromNetwork) {
      const map = new Map<string, string>(
        Object.entries(urls.downloadUrls).map(([key, value]) => [key, value.url])
      );

      chunkSource = new StorageChunkSource(this.storage, map);
    }

    if (!chunkSource) {
      throw new Error('No chunkSource found');
    }

    if (planToUse.newAndModifiedFiles.length) {
      this.changeState('reconstructing', options);
      await this.reconstruction.reconstructAll(planToUse, localDir, chunkSource, {
        forceRebuild: options?.force,
        verifyAfterRebuild: true,
        fileConcurrency: options?.fileReconstructionConcurrency,
        inPlaceReconstructionThreshold: options?.inPlaceReconstructionThreshold,
        onProgress: (reconstructProgress, diskSpeed, networkProgress, networkSpeed) => {
          this.updateProgress(reconstructProgress, 'reconstructing', diskSpeed, undefined, options);

          if (networkProgress) {
            this.updateProgress(networkProgress, 'download', 0, networkSpeed, options);
          }
        },
      });
    }

    if (planToUse.obsoleteChunks || planToUse.deletedFiles) {
      this.changeState('cleaning', options);
      await this.verifyAndDeleteObsoleteChunks(
        planToUse,
        localDir,
        remoteIndex,
        chunkSource,
        options
      );
    }

    await this.saveLocalIndex(localDir, remoteIndex);

    if (chunkSource instanceof DiskChunkSource) {
      chunkSource.clear();
    }
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

  async loadLocalIndex(localDir: string): Promise<RDIndex> {
    const localIndex = await this.delta.createIndexFromDirectory(
      localDir,
      this.config.chunkSize,
      this.config.maxConcurrency
    );
    return localIndex;
  }

  async downloadAllMissingChunks(
    downloadUrls: Record<string, ChunkUrlInfo>,
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

    const entries = Object.entries(downloadUrls);
    let completed = 0;
    let totalBytes = 0;

    const concurrency = this.config.maxConcurrency ?? 6;
    const queue = [...entries];

    let lastUpdateTime = Date.now();
    let lastBytes = 0;

    const worker = async () => {
      while (queue.length) {
        const [hash, info] = queue.pop()!;
        const readable = await this.storage.getChunkByUrl(info.url);

        if (!readable) {
          throw new Error(`Chunk missing: ${hash}`);
        }

        const buffer = await streamToBuffer(readable);
        chunkSource.setChunk(hash, buffer);

        completed++;
        totalBytes += buffer.length;

        const now = Date.now();
        const elapsed = now - lastUpdateTime;

        // each 100ms
        if (elapsed >= 100) {
          const bytesDiff = totalBytes - lastBytes;
          const speed = bytesDiff / (elapsed / 1000);

          lastUpdateTime = now;
          lastBytes = totalBytes;

          const percent = ((completed / entries.length) * 100).toFixed(1);
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

  async saveLocalIndex(localDir: string, index: RDIndex): Promise<void> {
    const dir = isAbsolute(localDir) ? localDir : resolve(process.cwd(), localDir);

    const indexPath = join(dir, 'rd-index.json');
    await writeFile(indexPath, JSON.stringify(index, null, 2), 'utf-8');
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
