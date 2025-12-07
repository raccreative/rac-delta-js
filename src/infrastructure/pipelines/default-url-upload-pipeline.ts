import { resolve, isAbsolute } from 'path';
import { open } from 'fs/promises';
import { Readable } from 'stream';

import { UploadOptions, UrlUploadPipeline } from '../../core/pipelines';
import { ChunkUrlInfo, RDIndex } from '../../core/models';
import { UrlStorageAdapter } from '../../core/adapters';
import { RacDeltaConfig } from '../../core/config';
import { Nullish } from '../../core/types';

export class DefaultUrlUploadPipeline extends UrlUploadPipeline {
  constructor(
    protected readonly storage: UrlStorageAdapter,
    protected readonly config: RacDeltaConfig
  ) {
    super(storage, config);
  }

  async execute(
    localIndex: RDIndex,
    urls: {
      uploadUrls: Record<string, ChunkUrlInfo>;
      deleteUrls?: Nullish<string[]>;
      indexUrl: string;
    },
    options?: Nullish<UploadOptions>
  ): Promise<RDIndex> {
    if (urls.uploadUrls.length) {
      this.changeState('uploading', options);
      await this.uploadMissingChunks(urls.uploadUrls, options);
    }

    if (urls.deleteUrls?.length) {
      this.changeState('cleaning', options);
      await this.deleteObsoleteChunks(urls.deleteUrls, options);
    }

    this.changeState('finalizing', options);
    await this.uploadIndex(localIndex, urls.indexUrl);

    return localIndex;
  }

  async uploadMissingChunks(
    uploadUrls: Record<string, ChunkUrlInfo>,
    options?: Nullish<UploadOptions>
  ): Promise<void> {
    const chunks = Object.values(uploadUrls);
    if (!chunks.length) {
      return;
    }

    const concurrency = this.config.maxConcurrency ?? 5;
    const grouped = this.groupChunksByFile(chunks);
    const totalChunks = chunks.length;

    let uploadedChunks = 0;
    let uploadedBytes = 0;
    const startTime = Date.now();

    const queue = [...grouped];

    const worker = async () => {
      while (queue.length) {
        const [filePath, fileChunks] = queue.pop()!;
        fileChunks.sort((a, b) => a.offset - b.offset);

        const finalFilePath = isAbsolute(filePath) ? filePath : resolve(process.cwd(), filePath);

        const fileHandle = await open(finalFilePath, 'r');
        const buffer = Buffer.alloc(this.config.chunkSize);

        try {
          for (const chunk of fileChunks) {
            const { url, offset, size } = chunk;
            const { bytesRead } = await fileHandle.read(buffer, 0, size, offset);
            const data = buffer.subarray(0, bytesRead);
            await this.storage.putChunkByUrl(url, Readable.from(data));

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

  async deleteObsoleteChunks(deleteUrls: string[], options?: UploadOptions) {
    if (!deleteUrls.length) {
      return;
    }

    const concurrency = this.config.maxConcurrency ?? 5;
    const queue = [...deleteUrls];
    const total = queue.length;
    let deleted = 0;

    if (total === 0) {
      return;
    }

    const failed: string[] = [];
    const maxRetries = 3;

    const worker = async () => {
      while (queue.length) {
        const url = queue.pop();

        if (!url) {
          break;
        }

        let success = false;

        for (let attempt = 1; attempt <= maxRetries && !success; attempt++) {
          try {
            await this.storage.deleteChunkByUrl(url);
            success = true;
          } catch (err) {
            if (attempt === maxRetries) {
              failed.push(url);
            } else {
              await new Promise((res) => setTimeout(res, 100 * attempt));
            }
          }
        }

        deleted++;
        this.updateProgress(deleted / total, 'deleting', undefined, options);
      }
    };

    await Promise.all(Array.from({ length: concurrency }, () => worker()));

    if (failed.length > 0) {
      throw new Error(`Failed to delete ${failed.length}/${total} chunks: ${failed.join(', ')}`);
    }
  }

  async uploadIndex(index: RDIndex, indexUrl: string) {
    await this.storage.putRemoteIndexByUrl(indexUrl, index);
  }

  private groupChunksByFile(chunks: ChunkUrlInfo[]): [string, ChunkUrlInfo[]][] {
    const groups = new Map<string, ChunkUrlInfo[]>();

    for (const chunk of chunks) {
      if (!groups.has(chunk.filePath)) {
        groups.set(chunk.filePath, []);
      }

      groups.get(chunk.filePath)!.push(chunk);
    }

    return Array.from(groups.entries());
  }
}
