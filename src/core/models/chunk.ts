export interface Chunk {
  hash: string;
  offset: number;
  size: number;
}

export interface ChunkUrlInfo {
  url: string;
  offset: number;
  size: number;
  filePath: string;
}
