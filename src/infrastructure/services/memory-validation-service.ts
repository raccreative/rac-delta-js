import { join, isAbsolute, resolve } from 'path';
import { stat } from 'fs/promises';

import { ValidationService, HasherService } from '../../core/services';
import { FileEntry, RDIndex } from '../../core/models';

export class MemoryValidationService implements ValidationService {
  constructor(private readonly hasher: HasherService) {}

  async validateFile(entry: FileEntry, path: string): Promise<boolean> {
    try {
      const finalPath = isAbsolute(path) ? path : resolve(process.cwd(), path);

      const stats = await stat(finalPath);

      return stats.size === entry.size && (await this.hasher.verifyFile(finalPath, entry.hash));
    } catch {
      return false;
    }
  }

  async validateIndex(index: RDIndex, basePath: string): Promise<boolean> {
    const directory = isAbsolute(basePath) ? basePath : resolve(process.cwd(), basePath);

    for (const file of index.files) {
      const filePath = join(directory, file.path);

      const valid = await this.validateFile(file, filePath);
      if (!valid) {
        return false;
      }
    }

    return true;
  }
}
