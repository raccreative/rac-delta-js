import { FileEntry } from './file-entry';
import { Chunk } from './chunk';

export interface ChunkEntry extends Chunk {
  filePath: string;
}

export interface DeltaPlan {
  newAndModifiedFiles: FileEntry[];
  deletedFiles: string[];
  missingChunks: ChunkEntry[];
  obsoleteChunks: ChunkEntry[];
}
