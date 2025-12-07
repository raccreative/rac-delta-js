import { AsyncChunkStream } from './delta-service';
import { Chunk, FileEntry } from '../models';

export interface StreamingHasher {
  update(data: Uint8Array | Buffer): void;
  // Hex only for now
  digest(encoding?: 'hex'): string;
}

export interface HasherService {
  /**
   * Will return a `FileEntry` of given file, calculating its hash and chunk hashes.
   *
   * @param filePath The relative path of the file (ex 'dir/file.txt').
   * @param rootDir The root dir where the index is. (ex 'dir').
   * @param chunkSize The size (in bytes) chunks will have, recommended is 1MB (1024 * 1024).
   *
   * **IMPORTANT NOTE:** selected chunkSize must be the same in all operations of rac-delta
   */
  hashFile(filePath: string, rootDir: string, chunkSize: number): Promise<FileEntry>;

  /**
   * Will process a stream of Chunks and return an array of hashed Chunks
   *
   * @param stream
   * @param onChunk callback that returns the processed bytes
   */
  hashStream(stream: AsyncChunkStream, onChunk?: (chunk: Uint8Array) => void): Promise<Chunk[]>;

  /**
   * Returns a hash of a buffer
   *
   * @param data
   */
  hashBuffer(data: Uint8Array): Promise<string>;

  /**
   * Verifies that a chunk has the expected hash
   *
   * @param data chunk data
   * @param expectedHash
   */
  verifyChunk(data: Uint8Array, expectedHash: string): Promise<boolean>;

  /**
   * Verifies that a file has the expected hash
   *
   * @param path file path
   * @param expectedHash
   */
  verifyFile(path: string, expectedHash: string): Promise<boolean>;

  createStreamingHasher(): Promise<StreamingHasher>;
}
