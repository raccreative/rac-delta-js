import { StorageConfig } from '../../core/config';
import { UnknownAny } from '../../core/types';

import { LocalStorageAdapter } from '../adapters/local-storage-adapter';
import { HTTPStorageAdapter } from '../adapters/http-storage-adapter';
import { DefaultUrlStorageAdapter } from '../adapters/url-storage-adapter';

import type { AzureBlobStorageAdapter } from '../adapters/azure-blob-storage-adapter';
import type { GCSStorageAdapter } from '../adapters/gcs-storage-adapter';
import type { SSHStorageAdapter } from '../adapters/ssh-storage-adapter';
import type { S3StorageAdapter } from '../adapters/s3-storage-adapter';

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
  static async create<C extends StorageConfig>(config: C): Promise<StorageAdapterByConfigType<C>> {
    switch (config.type) {
      case 'local': {
        return new LocalStorageAdapter(config) as StorageAdapterByConfigType<C>;
      }

      case 'http': {
        return new HTTPStorageAdapter(config) as StorageAdapterByConfigType<C>;
      }

      case 's3': {
        let S3StorageAdapter;
        try {
          ({ S3StorageAdapter } = await import('../adapters/s3-storage-adapter'));
        } catch {
          throw new Error(
            'Missing peer dependency: @aws-sdk/client-s3 module not found. Please install it.'
          );
        }
        return new S3StorageAdapter(config) as StorageAdapterByConfigType<C>;
      }

      case 'azure': {
        let AzureBlobStorageAdapter;
        try {
          ({ AzureBlobStorageAdapter } = await import('../adapters/azure-blob-storage-adapter'));
        } catch {
          throw new Error(
            'Missing peer dependency: "@azure/identity or @azure/storage-blob module not found. Please install them.'
          );
        }
        return new AzureBlobStorageAdapter(config) as StorageAdapterByConfigType<C>;
      }

      case 'gcs': {
        let GCSStorageAdapter;
        try {
          ({ GCSStorageAdapter } = await import('../adapters/gcs-storage-adapter'));
        } catch {
          throw new Error(
            'Missing peer dependency: @google-cloud/storage module not found. Please install it.'
          );
        }
        return new GCSStorageAdapter(config) as StorageAdapterByConfigType<C>;
      }

      case 'ssh': {
        let SSHStorageAdapter;
        try {
          ({ SSHStorageAdapter } = await import('../adapters/ssh-storage-adapter'));
        } catch {
          throw new Error('Missing peer dependency: ssh2 module not found. Please install it.');
        }
        return new SSHStorageAdapter(config) as StorageAdapterByConfigType<C>;
      }

      case 'url': {
        return new DefaultUrlStorageAdapter() as StorageAdapterByConfigType<C>;
      }

      default:
        throw new Error(`Unsupported storage type: ${(config as UnknownAny).type}`);
    }
  }
}
