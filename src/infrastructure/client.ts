import { RacDeltaConfig, StorageConfig } from '../core/config';
import {
  DeltaService,
  HasherService,
  ReconstructionService,
  ValidationService,
} from '../core/services';

import { PipelineBundleFor, PipelineFactory } from './factories/pipeline-factory';
import { AdapterFromConfig, StorageAdapterFactory } from './factories/storage-adpater-factory';
import { ServiceBundle, ServiceFactory } from './factories/service-factory';

/**
 * Main entry point of the RacDelta SDK.
 *
 * `RacDeltaClient` acts as a high-level orchestrator that initializes all
 * core services, adapters, and pipelines used for differential upload and
 * download operations.
 *
 * It is designed to provide a single, easy-to-use API for performing
 * file synchronization, hashing, validation, and reconstruction.
 *
 * ---
 * ### Example
 * ```ts
 * import { RacDeltaClient } from 'rac-delta';
 *
 * const client = new RacDeltaClient({
 *   storage: { type: 'local', basePath: './remote' },
 *   maxConcurrency: 4,
 * });
 *
 * const delta = await client.compareForDownload(localIndex, remoteIndex);
 * console.log(delta);
 * ```
 *
 * ---
 */
export class RacDeltaClient<C extends StorageConfig = StorageConfig> {
  readonly config: RacDeltaConfig & { storage: C };
  readonly storage: AdapterFromConfig<C>;
  readonly delta: DeltaService;
  readonly hasher: HasherService;
  readonly validation: ValidationService;
  readonly reconstruction: ReconstructionService;
  readonly pipelines: PipelineBundleFor<AdapterFromConfig<C>>;

  private constructor(
    config: RacDeltaConfig & { storage: C },
    storage: AdapterFromConfig<C>,
    services: ServiceBundle
  ) {
    this.config = config;
    this.storage = storage;

    this.delta = services.delta;
    this.hasher = services.hasher;
    this.validation = services.validation;
    this.reconstruction = services.reconstruction;

    this.pipelines = PipelineFactory.create(this.storage, services, config);
  }

  static async create<C extends StorageConfig>(
    config: RacDeltaConfig & { storage: C }
  ): Promise<RacDeltaClient<C>> {
    const storage = (await StorageAdapterFactory.create(
      config.storage
    )) as unknown as AdapterFromConfig<C>;

    const services = ServiceFactory.create();

    return new RacDeltaClient(config, storage, services);
  }
}
