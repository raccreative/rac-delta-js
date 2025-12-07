import { HashStorageAdapter, UrlStorageAdapter } from '../adapters';
import { ChunkUrlInfo, DeltaPlan, RDIndex } from '../models';
import { DeltaService } from '../services';
import { RacDeltaConfig } from '../config';
import { Nullish } from '../types';

export type UploadState = 'uploading' | 'comparing' | 'cleaning' | 'finalizing' | 'scanning';

export interface UploadOptions {
  /**
   * If true, forces complete upload even if remote index exists.
   * If false, only new and modified chunks will be uploaded.
   */
  force?: Nullish<boolean>;

  /**
   * If true and no remote index found, abort upload.
   * If false (default), uploads everything if no remote index found.
   */
  requireRemoteIndex?: Nullish<boolean>;

  /**
   * Files or directories that must be ignored when creating the rd-index.json.
   * Example: '*.ts', '/folder/*', 'ignorefile.txt'...
   */
  ignorePatterns?: Nullish<string[]>;

  /**
   * Optional callback to inform progress.
   */
  onProgress?: (type: 'upload' | 'deleting', progress: number, speed?: Nullish<number>) => void;

  /**
   * Optional callback for state changes.
   */
  onStateChange?: (state: UploadState) => void;
}

export abstract class UploadPipeline {
  protected updateProgress(
    value: number,
    state: 'upload' | 'deleting',
    speed?: Nullish<number>,
    options?: Nullish<UploadOptions>
  ) {
    options?.onProgress?.(state, value, speed);
  }

  protected changeState(state: UploadState, options?: UploadOptions) {
    options?.onStateChange?.(state);
  }
}

export abstract class HashUploadPipeline extends UploadPipeline {
  constructor(
    protected readonly storage: HashStorageAdapter,
    protected readonly delta: DeltaService,
    protected readonly config: RacDeltaConfig
  ) {
    super();
  }

  abstract execute(
    directory: string,
    remoteIndex?: Nullish<RDIndex>,
    options?: Nullish<UploadOptions>
  ): Promise<RDIndex>;

  abstract scanDirectory(dir: string, ignorePatterns?: Nullish<string[]>): Promise<RDIndex>;
  abstract uploadMissingChunks(
    plan: DeltaPlan,
    baseDir: string,
    force: boolean,
    options?: Nullish<UploadOptions>
  ): Promise<void>;
  abstract uploadIndex(index: RDIndex): Promise<void>;
  abstract deleteObsoleteChunks(plan: DeltaPlan, options?: Nullish<UploadOptions>): Promise<void>;
}

export abstract class UrlUploadPipeline extends UploadPipeline {
  constructor(
    protected readonly storage: UrlStorageAdapter,
    protected readonly config: RacDeltaConfig
  ) {
    super();
  }

  abstract execute(
    localIndex: RDIndex,
    urls: {
      uploadUrls: Record<string, ChunkUrlInfo>;
      deleteUrls?: Nullish<string[]>;
      indexUrl: string;
    },
    options?: Nullish<UploadOptions>
  ): Promise<RDIndex>;

  abstract uploadMissingChunks(
    uploadUrls: Record<string, ChunkUrlInfo>,
    options?: Nullish<UploadOptions>
  ): Promise<void>;
  abstract uploadIndex(index: RDIndex, uploadUrl: string): Promise<void>;
  abstract deleteObsoleteChunks(
    deleteUrls: string[],
    options?: Nullish<UploadOptions>
  ): Promise<void>;
}
