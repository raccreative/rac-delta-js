import {
  DeltaService,
  HasherService,
  ReconstructionService,
  ValidationService,
} from '../../core/services';

import { MemoryReconstructionService } from '../services/memory-reconstruction-service';
import { MemoryValidationService } from '../services/memory-validation-service';
import { HashWasmHasherService } from '../services/hash-wasm-hasher-service';
import { MemoryDeltaService } from '../services/memory-delta-service';

export interface ServiceBundle {
  delta: DeltaService;
  hasher: HasherService;
  validation: ValidationService;
  reconstruction: ReconstructionService;
}

export class ServiceFactory {
  static create(): ServiceBundle {
    const hasher = new HashWasmHasherService();
    const delta = new MemoryDeltaService(hasher);
    const validation = new MemoryValidationService(hasher);
    const reconstruction = new MemoryReconstructionService(hasher);

    return { delta, hasher, validation, reconstruction };
  }
}
