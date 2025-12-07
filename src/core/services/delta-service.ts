import { DeltaPlan, FileEntry, RDIndex } from '../models';
import { Nullish } from '../types';

export interface AsyncChunkStream extends AsyncIterable<Uint8Array> {
  nextChunk(): Promise<Uint8Array | null>;
  reset?(): Promise<void>;
  close?(): Promise<void>;
}

export interface DeltaService {
  /**
   * Creates an RDIndex for a given directory.
   *
   * This process scans all files recursively, splits them into chunks,
   * hashes each chunk (using HasherService.hashStream), and builds
   * the rd-index.json structure.
   *
   * @param rootPath the path of the directory
   * @param chunkSize The size (in bytes) chunks will have, recommended is 1024.
   *
   */
  createIndexFromDirectory(
    rootPath: string,
    chunkSize: number,
    concurrency?: Nullish<number>,
    ignorePatterns?: Nullish<string[]>
  ): Promise<RDIndex>;

  /**
   * Creates a FileEntry from a readable data stream.
   *
   * This method is used when the data source is remote or does not
   * exist as a physical file in the local filesystem.
   *
   * It reads the stream in chunks (chunk size must be defined by source), hashes each one (using HasherService.hashStream),
   * and produces a `FileEntry` compatible with RDIndex.
   *
   * @param stream An async stream providing file chunks.
   * @param path Relative path of the source file
   */
  createFileEntryFromStream(stream: AsyncChunkStream, path: string): Promise<FileEntry>;

  /**
   * Compare two rd-index.json and generate a DeltaPlan. (Neutral method, for more specific methods use compareForUpload or compareForDownload)
   *
   * @param source The rd-index from source to compare (Example: local)
   * @param target The rd-index from target to compare (Example: remote server)
   *
   * Local -> Remote = upload comparison.
   *
   * Remote -> Local = download comparison.
   *
   * DeltaPlan Explanation:
   *  - missing chunks: chunks that exist in `source` but are missing in `target`
   *                   (i.e. need to be transferred from source -> target).
   *  - reused chunks: chunks present in target that can be reused.
   *
   *  - obsolete chunks: chunks that no longer exists in source and needs to be removed from target.
   *    (considerations must be taken for deduplication, as for uploads, if a chunk is used for multiple files it could not be marked as obsolete)
   */
  compare(source: RDIndex, target: RDIndex | null): DeltaPlan;
  /**
   * Merges 2 `DeltaPlan`
   *
   * @param base base `DeltaPlan`
   * @param updates `DeltaPlan` to merge with base
   */
  mergePlans(base: DeltaPlan, updates: DeltaPlan): DeltaPlan;

  /**
   * This wrapper will compare rd-indexes ready for upload update. It can check deduplication correctly with obsolete chunks.
   * (Example: 1 chunk is used in 2 files but one file no longer uses the chunk => it IS NOT A OBSOLETE CHUNK)
   *
   * @param localIndex the local rd-index
   * @param remoteIndex the remote rd-index (null to upload everything)
   */
  compareForUpload(localIndex: RDIndex, remoteIndex: RDIndex | null): Promise<DeltaPlan>;
  /**
   * This wrapper will compare rd-indexes ready for download update
   *
   * @param localIndex the local rd-index (null to download everything)
   * @param remoteIndex the remote rd-index
   */
  compareForDownload(localIndex: RDIndex | null, remoteIndex: RDIndex): Promise<DeltaPlan>;
}
