import { Chunk } from './chunk';

export interface FileEntry {
  path: string;
  size: number;
  hash: string;
  modifiedAt: number;
  chunks: Chunk[];
}
