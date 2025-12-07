import { Readable } from 'stream';
import { Storage } from '@google-cloud/storage';

import { BlobInfo, HashStorageAdapter } from '../../core/adapters';
import { GCSStorageConfig } from '../../core/config';
import { RDIndex } from '../../core/models';

export class GCSStorageAdapter extends HashStorageAdapter {
  private storage: Storage;

  constructor(private readonly config: GCSStorageConfig) {
    super();

    this.storage = new Storage({
      projectId: config.credentials.projectId,
      apiEndpoint: config.apiEndpoint,
      credentials: {
        client_email: config.credentials.clientEmail,
        private_key: config.credentials.privateKey,
      },
    });
  }

  async dispose(): Promise<void> {
    return;
  }

  private getPath(hash: string): string {
    return this.config.pathPrefix ? `${this.config.pathPrefix}/chunks/${hash}` : hash;
  }

  async getChunk(hash: string): Promise<Readable | null> {
    const file = this.storage.bucket(this.config.bucket).file(this.getPath(hash));
    const [exists] = await file.exists();

    if (!exists) {
      return null;
    }

    return file.createReadStream();
  }

  async putChunk(hash: string, data: Readable): Promise<void> {
    const file = this.storage.bucket(this.config.bucket).file(this.getPath(hash));

    const writeStream = file.createWriteStream({ resumable: false });

    await new Promise<void>((resolve, reject) => {
      data.pipe(writeStream).on('finish', resolve).on('error', reject);
    });
  }

  async chunkExists(hash: string): Promise<boolean> {
    const file = this.storage.bucket(this.config.bucket).file(this.getPath(hash));

    const [exists] = await file.exists();

    return exists;
  }

  async deleteChunk(hash: string): Promise<void> {
    const file = this.storage.bucket(this.config.bucket).file(this.getPath(hash));
    await file.delete({ ignoreNotFound: true });
  }

  async listChunks(): Promise<string[]> {
    const chunksPath = this.config.pathPrefix ? `${this.config.pathPrefix}/chunks` : '';

    const [files] = await this.storage.bucket(this.config.bucket).getFiles({
      prefix: chunksPath,
    });

    return files.map((file) => file.name.replace(`${this.config.pathPrefix}/chunks/`, ''));
  }

  async getChunkInfo(hash: string): Promise<BlobInfo | null> {
    const file = this.storage.bucket(this.config.bucket).file(this.getPath(hash));
    const [exists] = await file.exists();

    if (!exists) {
      return null;
    }

    const [metadata] = await file.getMetadata();

    const meta = metadata.metadata
      ? Object.fromEntries(
          Object.entries(metadata.metadata).map(([key, value]) => [key, JSON.stringify(value)])
        )
      : {};

    return {
      hash,
      size: Number(metadata.size),
      modified: metadata.updated ? new Date(metadata.updated) : undefined,
      metadata: meta,
    };
  }

  async getRemoteIndex(): Promise<RDIndex | null> {
    const indexPath = this.config.pathPrefix
      ? `${this.config.pathPrefix}/rd-index.json`
      : 'rd-index.json';

    const file = this.storage.bucket(this.config.bucket).file(indexPath);
    const [exists] = await file.exists();

    if (!exists) {
      return null;
    }

    const [contents] = await file.download();
    const data = contents.toString('utf-8');

    return JSON.parse(data) as RDIndex;
  }

  async putRemoteIndex(index: RDIndex): Promise<void> {
    const indexPath = this.config.pathPrefix
      ? `${this.config.pathPrefix}/rd-index.json`
      : 'rd-index.json';

    const bucket = this.storage.bucket(this.config.bucket);
    const file = bucket.file(indexPath);

    const contents = JSON.stringify(index, null, 2);

    await file.save(contents, {
      contentType: 'application/json',
      resumable: false,
    });
  }
}
