import { Readable } from 'stream';

import { FileEntry, DeltaPlan } from '../models';
import { Nullish } from '../types';

export const DEFAULT_IN_PLACE_RECONSTRUCTION_THRESHOLD = 400 * 1024 * 1024; // 400MB

export interface ReconstructionOptions {
  /**
   * Force to rebuild even if hash file matches.
   */
  forceRebuild?: Nullish<boolean>;

  /**
   * Verifies the reconstructed file hash after finishing.
   * If hash does not match, an error is thrown.
   */
  verifyAfterRebuild?: Nullish<boolean>;

  /**
   * Minimum file size (in bytes) required to perform an **in-place reconstruction** instead of using a temporary file.
   * Default: `400 * 1024 * 1024` (400 MB).
   *
   * **In-place reconstruction:**
   * The existing file is opened and updated directly by overwriting only the modified or missing chunks.
   *
   * **.tmp reconstruction:**
   * The file is fully rebuilt in a temporary `.tmp` location using all chunks (new and existing), then replaced over the original file.
   *
   * **When to use:**
   * In-place reconstruction is recommended for **large files**, as it avoids rewriting the entire file and significantly reduces disk space usage.
   * However, it may be **unsafe for certain formats** (e.g., ZIP archives or databases) that are sensitive to partial writes or corruption.
   * To disable in-place reconstruction entirely, set this value to `0`.
   */
  inPlaceReconstructionThreshold?: Nullish<number>;

  /**
   * How many files will reconstruct concurrently (default value is 5)
   */
  fileConcurrency?: Nullish<number>;

  /**
   * Callback that returns disk usage and optional network speed (only for storage chunk sources via streaming download-reconstruction)
   *
   * @param reconstructProgress current reconstruction progress
   * @param diskSpeed speed of disk write in bytes per second
   * @param networkProgress current network progress if any
   * @param networkSpeed download speed in bytes per second
   */
  onProgress?: (
    reconstructProgress: number,
    diskSpeed: number,
    networkProgress?: Nullish<number>,
    networkSpeed?: Nullish<number>
  ) => void;
}

export interface ChunkSource {
  /**
   * Gets a chunk from the source.
   */
  getChunk(hash: string): Promise<Buffer>;

  /**
   * Retrieves multiple chunks concurrently.
   */
  getChunks?(
    hashes: string[],
    options?: Nullish<{ concurrency?: number }>
  ): Promise<Map<string, Buffer>>;

  /**
   * Streams file chunks from storage concurrently.
   * Can preserve original order or emit as workers complete.
   *
   * @param options.concurrency Number of parallel fetches (default 8)
   * @param options.preserveOrder Whether to yield in input order (default true)
   */
  streamChunks?(
    hashes: string[],
    options?: Nullish<{ concurrency?: number; preserveOrder?: boolean }>
  ): AsyncGenerator<{ hash: string; data: Readable }>;
}

export interface ReconstructionService {
  /**
   * Reconstructs a file in disk.
   * Able to reconstruct a new file or an existing file.
   *
   * @param entry The `FileEntry` containing the list of chunks and path of the file
   * @param outputPath The path where the file will be reconstructed.
   * @param chunkSource the source implementations of the chunks
   * @param options optional parameters for the reconstruction
   */
  reconstructFile(
    entry: FileEntry,
    outputPath: string,
    chunkSource: ChunkSource,
    options?: Nullish<ReconstructionOptions>
  ): Promise<void>;

  /**
   * Reconstructs all files from a DeltaPlan in disk.
   *
   * @param plan The DeltaPlan containing the list of files and chunks.
   * @param outputDir The dir where the files will be reconstructed.
   * @param chunkSource the source implementations of the chunks
   * @param options optional parameters for the reconstruction
   */
  reconstructAll(
    plan: DeltaPlan,
    outputDir: string,
    chunkSource: ChunkSource,
    options?: Nullish<ReconstructionOptions>
  ): Promise<void>;

  /**
   * Reconstructs a file to stream.
   *
   * @param entry The FileEntry containing the list of chunks of the file
   * @param chunkSource the source implementations of the chunks
   */
  reconstructToStream(entry: FileEntry, chunkSource: ChunkSource): Promise<Readable>;
}
