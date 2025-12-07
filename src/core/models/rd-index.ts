import { FileEntry } from './file-entry';

export interface RDIndex {
  version: number;
  createdAt: number;
  chunkSize: number;
  files: FileEntry[];
}
