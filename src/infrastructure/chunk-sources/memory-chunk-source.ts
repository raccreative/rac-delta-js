import { ChunkNotFoundException } from '../../core/exceptions';
import { ChunkSource } from '../../core/services';

export class MemoryChunkSource implements ChunkSource {
  private cache = new Map<string, Buffer>();

  async getChunk(hash: string): Promise<Buffer> {
    const chunk = this.cache.get(hash);

    if (!chunk) {
      throw new ChunkNotFoundException(`${hash} not found in memory`);
    }

    return chunk;
  }

  async getChunks(hashes: string[]): Promise<Map<string, Buffer>> {
    const results = new Map<string, Buffer>();

    for (const hash of hashes) {
      const chunk = await this.getChunk(hash);
      results.set(hash, chunk as Buffer);
    }

    return results;
  }

  hasChunk(hash: string): boolean {
    return this.cache.has(hash);
  }

  setChunk(hash: string, data: Buffer): void {
    this.cache.set(hash, data);
  }
}
