import { FileEntry, RDIndex } from '../models';

export interface ValidationService {
  /**
   * Will validate a given file with its `FileEntry`
   *
   * @param entry
   * @param path path of the file to validate
   */
  validateFile(entry: FileEntry, path: string): Promise<boolean>;

  /**
   * Will validate all files of a `RdIndex`
   *
   * @param index rd-index
   * @param basePath directory of the files
   */
  validateIndex(index: RDIndex, basePath: string): Promise<boolean>;
}
