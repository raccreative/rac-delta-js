import { Nullish } from '../types';

export interface BaseStorageConfig {
  /**
   * Example: my-prefix/updates/42
   */
  pathPrefix?: Nullish<string>;
}

export interface S3StorageConfig extends BaseStorageConfig {
  type: 's3';
  endpoint?: Nullish<string>;
  region?: Nullish<string>;
  bucket: string;
  credentials: {
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: Nullish<string>;
    expiration?: Nullish<Date>;
  };
}

export interface AzureBlobStorageConfig extends BaseStorageConfig {
  type: 'azure';
  container: string;
  endpoint: string;
  credentials: {
    accountName?: Nullish<string>;
    accountKey?: Nullish<string>;
    sasToken?: Nullish<string>;
  };
}

export interface GCSStorageConfig extends BaseStorageConfig {
  type: 'gcs';
  bucket: string;
  apiEndpoint?: Nullish<string>;
  credentials: {
    projectId: string;
    clientEmail?: Nullish<string>;
    privateKey?: Nullish<string>;
  };
}

export interface HTTPStorageConfig extends BaseStorageConfig {
  type: 'http';

  /**
   * Base URL used for all chunk operations (`GET`, `PUT`, `DELETE`, `HEAD`).
   *
   * Each chunk will be accessed as:
   * ```
   * {endpoint}/{pathPrefix?}/{chunk-hash}
   * ```
   *
   * Example:
   * ```
   * endpoint: "https://ducks.com/api"
   * pathPrefix: "uploads"
   *
   * => https://ducks.com/api/uploads/{chunk-hash}
   * ```
   *
   * This should point to an HTTP server or compatible API that supports
   * binary upload and download via standard HTTP verbs.
   */
  endpoint: string;

  /**
   * Optional path (relative to `endpoint` and `pathPrefix`) where the remote
   * index file (`rd-index.json`) can be found.
   *
   * If omitted, the adapter will automatically look for:
   * ```
   * {endpoint}/{pathPrefix?}/rd-index.json
   * ```
   *
   * Example:
   * ```
   * indexFilePath: "index"
   * => https://ducks.com/api/index
   *
   * indexFilePath: "metadata/rd-index.json"
   * => https://ducks.com/api/metadata/rd-index.json
   * ```
   */
  indexFilePath?: Nullish<string>;

  /**
   * Optional credentials used for authenticated HTTP requests.
   *
   * Supports both Bearer tokens and API keys.
   *
   * Example:
   * ```
   * credentials: {
   *   bearerToken: "eyJhbGciOiJIUzI1...",
   *   apiKey: "my-secret-key"
   * }
   * ```
   *
   * The adapter automatically includes the corresponding headers:
   * - `Authorization: Bearer <token>`
   * - `x-api-key: <apiKey>`
   */
  credentials?: Nullish<{
    bearerToken?: Nullish<string>;
    apiKey?: Nullish<string>;
  }>;
}

export interface URLStorageConfig extends BaseStorageConfig {
  type: 'url';
}

export interface SSHStorageConfig extends BaseStorageConfig {
  type: 'ssh';
  host: string;
  port?: Nullish<number>;
  credentials: {
    username: string;
    password?: Nullish<string>;
    privateKey?: Nullish<string>;
  };
}

export interface LocalStorageConfig extends BaseStorageConfig {
  type: 'local';
  basePath: string;
}

export type StorageConfig =
  | S3StorageConfig
  | AzureBlobStorageConfig
  | GCSStorageConfig
  | HTTPStorageConfig
  | SSHStorageConfig
  | LocalStorageConfig
  | URLStorageConfig;

export interface RacDeltaConfig {
  /**
   * Recommended: 1MB
   */
  chunkSize: number;
  /**
   * Max concurrency of workers like upload chunks, delete chunks, etc...
   */
  maxConcurrency?: Nullish<number>;
  storage: StorageConfig;
}
