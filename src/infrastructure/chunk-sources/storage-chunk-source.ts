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
      maxPrefetch = 12,
    }: { concurrency?: number; preserveOrder?: boolean; maxPrefetch?: number } = {}
  ): AsyncGenerator<{ hash: string; data: Readable }> {
    if (hashes.length === 0) {
      return;
    }

    const queue = hashes.map((hash, index) => ({ hash, index }));

    const results = new Map<number, { hash: string; data: Readable }>();
    let nextIndexToEmit = 0;
    let activeWorkers = 0;
    let workersDone = false;

    const pendingResolvers: (() => void)[] = [];
    const pendingSlotResolvers: (() => void)[] = [];

    let workerError: Error | null = null;

    const signalNext = () => {
      const resolver = pendingResolvers.shift();
      if (resolver) {
        resolver();
      }
    };

    const signalSlot = () => {
      const resolver = pendingSlotResolvers.shift();
      if (resolver) {
        resolver();
      }
    };

    const waitForData = async () => {
      while (
        (preserveOrder && !results.has(nextIndexToEmit) && !workerError) ||
        (!preserveOrder && results.size === 0 && !workerError)
      ) {
        await new Promise<void>((resolve) => pendingResolvers.push(resolve));
      }
    };

    const worker = async () => {
      activeWorkers++;

      try {
        while (queue.length > 0 && !workerError) {
          while (results.size >= maxPrefetch && !workerError) {
            await new Promise<void>((resolve) => pendingSlotResolvers.push(resolve));
          }

          if (workerError) {
            return;
          }

          const item = queue.shift();
          if (!item) {
            continue;
          }

          const { hash, index } = item;

          try {
            const stream =
              this.storage.type === 'hash'
                ? await (this.storage as unknown as HashStorageAdapter).getChunk(hash)
                : await (this.storage as unknown as UrlStorageAdapter).getChunkByUrl(
                    this.urlsMap?.get(hash)!
                  );

            if (!stream) {
              throw new ChunkNotFoundException(`Chunk ${hash} not found in storage`);
            }

            stream.once('error', (err) => {
              workerError = err instanceof Error ? err : new Error(String(err));
              signalNext();
            });

            results.set(index, { hash, data: stream });
            signalNext();
          } catch (err) {
            workerError = err instanceof Error ? err : new Error(String(err));
            signalNext();
            return;
          }
        }
      } finally {
        activeWorkers--;

        if (activeWorkers === 0) {
          workersDone = true;
        }

        signalNext();
        signalSlot();
      }
    };

    const workers = Array.from({ length: Math.min(concurrency, queue.length) }, worker);

    try {
      while (true) {
        await waitForData();

        if (workerError) {
          await Promise.allSettled(workers);
          throw workerError;
        }

        if (preserveOrder) {
          while (results.has(nextIndexToEmit)) {
            yield results.get(nextIndexToEmit)!;
            results.delete(nextIndexToEmit);
            signalSlot();
            nextIndexToEmit++;
          }
        }

        if (!preserveOrder) {
          const [index, value] = results.entries().next().value ?? [];

          if (value !== undefined && index !== undefined) {
            yield value;
            results.delete(index);
            signalSlot();
          }
        }

        if (workersDone && results.size === 0) {
          break;
        }
      }
    } finally {
      await Promise.allSettled(workers);
    }
  }
}
