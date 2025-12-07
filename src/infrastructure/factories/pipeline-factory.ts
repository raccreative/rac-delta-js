import {
  HashDownloadPipeline,
  HashUploadPipeline,
  UrlDownloadPipeline,
  UrlUploadPipeline,
} from '../../core/pipelines';
import { RacDeltaConfig } from '../../core/config';
import { HashStorageAdapter, StorageAdapter, UrlStorageAdapter } from '../../core/adapters';

import { DefaultHashDownloadPipeline } from '../pipelines/default-hash-download-pipeline';
import { DefaultUrlDownloadPipeline } from '../pipelines/default-url-download-pipeline';
import { DefaultHashUploadPipeline } from '../pipelines/default-hash-upload-pipeline';
import { DefaultUrlUploadPipeline } from '../pipelines/default-url-upload-pipeline';

import { ServiceBundle } from './service-factory';

export type PipelineBundleFor<S extends StorageAdapter> = S['type'] extends 'hash'
  ? {
      upload: HashUploadPipeline;
      download: HashDownloadPipeline;
    }
  : S['type'] extends 'url'
    ? {
        upload: UrlUploadPipeline;
        download: UrlDownloadPipeline;
      }
    : never;

export class PipelineFactory {
  static create<S extends StorageAdapter>(
    storage: S,
    services: ServiceBundle,
    config: RacDeltaConfig
  ): PipelineBundleFor<S> {
    if (storage instanceof HashStorageAdapter) {
      return {
        upload: new DefaultHashUploadPipeline(storage, services.delta, config),
        download: new DefaultHashDownloadPipeline(
          services.reconstruction,
          services.validation,
          storage,
          config,
          services.delta
        ),
      } as unknown as PipelineBundleFor<S>;
    }

    if (storage instanceof UrlStorageAdapter) {
      return {
        upload: new DefaultUrlUploadPipeline(storage, config),
        download: new DefaultUrlDownloadPipeline(
          storage,
          services.reconstruction,
          services.validation,
          services.delta,
          config
        ),
      } as unknown as PipelineBundleFor<S>;
    }

    throw new Error(`Unsupported storage adapter type: ${storage.type}`);
  }
}
