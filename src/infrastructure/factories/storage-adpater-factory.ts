import { StorageConfig } from '../../core/config';

import { AzureBlobStorageAdapter } from '../adapters/azure-blob-storage-adapter';
import { DefaultUrlStorageAdapter } from '../adapters/url-storage-adapter';
import { LocalStorageAdapter } from '../adapters/local-storage-adapter';
import { HTTPStorageAdapter } from '../adapters/http-storage-adapter';
import { GCSStorageAdapter } from '../adapters/gcs-storage-adapter';
import { SSHStorageAdapter } from '../adapters/ssh-storage-adapter';
import { S3StorageAdapter } from '../adapters/s3-storage-adapter';
import { UnknownAny } from '../../core/types';

export type StorageAdapterMap = {
  local: LocalStorageAdapter;
  http: HTTPStorageAdapter;
  s3: S3StorageAdapter;
  azure: AzureBlobStorageAdapter;
  gcs: GCSStorageAdapter;
  ssh: SSHStorageAdapter;
  url: DefaultUrlStorageAdapter;
};

export type StorageTypeKey = keyof StorageAdapterMap;

export type AdapterFromConfig<T extends StorageConfig> = T extends { type: StorageTypeKey }
  ? StorageAdapterMap[T['type']]
  : never;

type StorageAdapterByConfigType<C extends StorageConfig> = C extends { type: 'local' }
  ? LocalStorageAdapter
  : C extends { type: 'http' }
    ? HTTPStorageAdapter
    : C extends { type: 's3' }
      ? S3StorageAdapter
      : C extends { type: 'azure' }
        ? AzureBlobStorageAdapter
        : C extends { type: 'gcs' }
          ? GCSStorageAdapter
          : C extends { type: 'ssh' }
            ? SSHStorageAdapter
            : C extends { type: 'url' }
              ? DefaultUrlStorageAdapter
              : never;

export class StorageAdapterFactory {
  static create<C extends StorageConfig>(config: C): StorageAdapterByConfigType<C> {
    switch (config.type) {
      case 'local':
        return new LocalStorageAdapter(config) as StorageAdapterByConfigType<C>;
      case 'http':
        return new HTTPStorageAdapter(config) as StorageAdapterByConfigType<C>;
      case 's3':
        return new S3StorageAdapter(config) as StorageAdapterByConfigType<C>;
      case 'azure':
        return new AzureBlobStorageAdapter(config) as StorageAdapterByConfigType<C>;
      case 'gcs':
        return new GCSStorageAdapter(config) as StorageAdapterByConfigType<C>;
      case 'ssh':
        return new SSHStorageAdapter(config) as StorageAdapterByConfigType<C>;
      case 'url':
        return new DefaultUrlStorageAdapter() as StorageAdapterByConfigType<C>;
      default:
        throw new Error(`Unsupported storage type: ${(config as UnknownAny).type}`);
    }
  }
}
