import { HashStorageAdapter, StorageAdapter, UrlStorageAdapter } from '../../core/adapters';
import { streamToBuffer } from '../../core/utils/stream-to-buffer';
import { ChunkNotFoundException } from '../../core/exceptions';
import { ChunkSource } from '../../core/services';
import { Nullish } from '../../core/types';

import { Readable, PassThrough } from 'stream';

export class StorageChunkSource implements ChunkSource {
  constructor(
    private readonly storage: StorageAdapter,
    private readonly urlsMap?: Nullish<Map<string, string>>
  ) {}

  async getChunk(hash: string): Promise<Buffer> {
    const url = this.urlsMap?.get(hash);

    if (!url && this.storage.type === 'url') {
      throw new Error(`No URL found for hash: ${hash}`);
    }

    const stream =
      this.storage.type === 'hash'
        ? await (this.storage as unknown as HashStorageAdapter).getChunk(hash)
        : await (this.storage as unknown as UrlStorageAdapter).getChunkByUrl(url as string);

    if (!stream) {
      throw new ChunkNotFoundException(`${hash} not found in storage`);
    }

    return streamToBuffer(stream);
  }

  async getChunks(
    hashes: string[],
    { concurrency = 8 }: { concurrency?: number } = {}
  ): Promise<Map<string, Buffer>> {
    const results = new Map<string, Buffer>();
    const queue = [...hashes];

    const workers = Array.from({ length: concurrency }).map(async () => {
      while (queue.length > 0) {
        const hash = queue.shift();
        if (!hash) {
          break;
        }

        const data = await this.getChunk(hash);
        results.set(hash, data);
      }
    });

    await Promise.all(workers);
    return results;
  }

  async *streamChunks(
    hashes: string[],
    {
      concurrency = 4,
      preserveOrder = true,
    }: { concurrency?: number; preserveOrder?: boolean } = {}
  ): AsyncGenerator<{ hash: string; data: Readable }> {
    if (hashes.length === 0) {
      return;
    }

    const controller = new AbortController();
    const queue = hashes.map((hash, index) => ({ hash, index }));

    const results = new Map<number, { hash: string; data: Readable }>();
    let nextIndexToEmit = 0;
    let activeWorkers = 0;
    let workersDone = false;

    const pendingResolvers: (() => void)[] = [];

    let workerError: Error | null = null;

    const signalNext = () => {
      const resolver = pendingResolvers.shift();
      if (resolver) {
        resolver();
      }
    };

    const waitForData = async () => {
      while (
        (preserveOrder && !results.has(nextIndexToEmit) && workerError === null) ||
        (!preserveOrder && results.size === 0 && workerError === null)
      ) {
        await new Promise<void>((resolve) => pendingResolvers.push(resolve));
      }
    };

    const worker = async () => {
      activeWorkers++;

      try {
        while (queue.length > 0 && !controller.signal.aborted) {
          const { hash, index } = queue.shift()!;

          try {
            const url = this.urlsMap?.get(hash);

            if (!url && this.storage.type === 'url') {
              throw new Error(`No URL found for hash: ${hash}`);
            }

            const stream =
              this.storage.type === 'hash'
                ? await (this.storage as unknown as HashStorageAdapter).getChunk(hash)
                : await (this.storage as unknown as UrlStorageAdapter).getChunkByUrl(url as string);

            if (!stream) {
              throw new ChunkNotFoundException(`${hash} not found in storage`);
            }

            const pass = new PassThrough({ highWaterMark: 64 * 1024 });

            stream.on('error', (err) => {
              workerError = err instanceof Error ? err : new Error(String(err));
              controller.abort();
              signalNext();
            });

            pass.on('error', (err) => {
              workerError = err instanceof Error ? err : new Error(String(err));
              controller.abort();
              signalNext();
            });

            stream.pipe(pass);
            results.set(index, { hash, data: pass });
            signalNext();
          } catch (error) {
            workerError = error instanceof Error ? error : new Error(String(error));
            controller.abort();
            signalNext();
            return;
          }
        }
      } finally {
        activeWorkers--;

        if (activeWorkers === 0) {
          workersDone = true;
          signalNext();
        }
      }
    };

    const workers = Array.from({ length: Math.min(concurrency, queue.length) }, worker);

    try {
      while (true) {
        await waitForData();

        if (workerError) {
          // ensure workers settle so their promise rejections don't become unhandled
          await Promise.allSettled(workers);
          throw workerError;
        }

        // Emit strictly in order
        if (preserveOrder) {
          while (results.has(nextIndexToEmit)) {
            yield results.get(nextIndexToEmit)!;
            results.delete(nextIndexToEmit);
            nextIndexToEmit++;
          }
        }

        // Emit as soon as any result is ready
        if (!preserveOrder) {
          const [index, value] = results.entries().next().value ?? [];

          if (value !== undefined && index !== undefined) {
            yield value;
            results.delete(index);
          }
        }

        if (workersDone && results.size === 0) {
          break;
        }
      }
    } finally {
      controller.abort();
      await Promise.allSettled(workers);
    }
  }
}
