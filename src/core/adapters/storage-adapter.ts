import { Readable } from 'stream';

import { Nullish } from '../types';
import { RDIndex } from '../models';

export interface BlobInfo {
  hash: string;
  size: number;
  modified?: Nullish<Date>;
  metadata?: Nullish<Record<string, string>>;
}

export abstract class StorageAdapter {
  abstract readonly type: 'hash' | 'url';

  abstract dispose(): Promise<void>;
}

export abstract class HashStorageAdapter extends StorageAdapter {
  readonly type = 'hash' as const;

  /**
   * Returns a readable stream of the chunk if exists, null otherwise.
   *
   * @param hash the hash of the chunk
   */
  abstract getChunk(hash: string): Promise<Readable | null>;

  /**
   * Upload a chunk from a readable stream
   *
   * @param hash the hash of the chunk
   * @param data the `Readable` stream
   * @param opts.overwrite whether to overwrite chunk if exists or not (depends on the implementation)
   * @param opts.size ContentLength of the chunk, sometimes needed for some adapters
   */
  abstract putChunk(
    hash: string,
    data: Readable,
    opts?: Nullish<{ overwrite?: Nullish<boolean>; size?: Nullish<number> }>
  ): Promise<void>;

  /**
   * Check if chunk exists in the storage
   *
   * @param hash the hash of the chunk
   */
  abstract chunkExists(hash: string): Promise<boolean>;

  /**
   * Deletes a chunk identified by hash
   *
   * @param hash the hash of the chunk to delete
   */
  abstract deleteChunk(hash: string): Promise<void>;

  /**
   * List available chunks (optional)
   *
   * Chunks are always under given-prefix/chunks/
   */
  listChunks?(): Promise<string[]>;

  /**
   * Returns chunk info
   *
   * @param hash the hash of the chunk
   */
  getChunkInfo?(hash: string): Promise<BlobInfo | null>;

  /**
   * Helper to get rd-index.json
   * Should return null if not found.
   *
   * rd-index is always on given-prefix/rd-index.json (root)
   */
  abstract getRemoteIndex(): Promise<RDIndex | null>;

  /**
   * Helper to upload rd-index.json
   *
   * rd-index is always on given-prefix/rd-index.json (root)
   */
  abstract putRemoteIndex(index: RDIndex): Promise<void>;
}

export abstract class UrlStorageAdapter extends StorageAdapter {
  readonly type = 'url' as const;

  /**
   * Returns a readable stream of the chunk if exists by url, null otherwise.
   *
   * @param url the url to get the chunk (example: signed S3 url)
   */
  abstract getChunkByUrl(url: string): Promise<Readable | null>;

  /**
   * Upload a chunk from a readable stream by url
   *
   * @param url the url to put the chunk (example: signed S3 url)
   * @param data the data stream `Readable`
   */
  abstract putChunkByUrl(url: string, data: Readable): Promise<void>;

  /**
   * Deletes a chunk by url.
   *
   * @param url url to delete the chunk (example: signed S3 url)
   */
  abstract deleteChunkByUrl(url: string): Promise<void>;

  /**
   * Check if chunk exists in the storage by url
   *
   * @param url url to check the chunk (example: signed S3 url)
   */
  abstract chunkExistsByUrl(url: string): Promise<boolean>;

  /**
   * List available chunks by url (example: signed S3 url)
   */
  listChunksByUrl?(url: string): Promise<string[]>;

  /**
   * Returns chunk info by url (example: signed S3 url)
   *
   * @param hash the hash of the chunk
   * @param url the url to get the chunk info
   */
  getChunkInfoByUrl?(hash: string, url: string): Promise<BlobInfo | null>;

  /**
   * Get rd-index.json by url (example: signed S3 url)
   * Should return null if not found or not supported.
   *
   * @param url the url to get the index
   */
  abstract getRemoteIndexByUrl(url: string): Promise<RDIndex | null>;

  /**
   * Put rd-index.json by url (example: signed S3 url)
   *
   * @param url the url to put the index
   * @param index the `RDIndex` object
   */
  abstract putRemoteIndexByUrl(url: string, index: RDIndex): Promise<void>;
}
