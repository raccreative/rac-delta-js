export class GetChunkException extends Error {
  constructor(message: string) {
    super(`Failed to fetch Chunk: ${message}`);
  }
}

export class PutChunkException extends Error {
  constructor(message: string) {
    super(`Failed to upload Chunk: ${message}`);
  }
}

export class DeleteChunkException extends Error {
  constructor(message: string) {
    super(`Failed to delete Chunk: ${message}`);
  }
}

export class HeadChunkException extends Error {
  constructor(message: string) {
    super(`Failed to HEAD Chunk: ${message}`);
  }
}

export class GetRemoteIndexException extends Error {
  constructor(message: string) {
    super(`Failed to get index: ${message}`);
  }
}

export class PutRemoteIndexException extends Error {
  constructor(message: string) {
    super(`Failed to upload index: ${message}`);
  }
}

export class ChunkAlreadyExistsException extends Error {
  constructor(message: string) {
    super(`Chunk already exists: ${message}`);
  }
}

export class ChunkNotFoundException extends Error {
  constructor(message: string) {
    super(`Chunk not found: ${message}`);
  }
}
