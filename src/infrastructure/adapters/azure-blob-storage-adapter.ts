import { Readable } from 'stream';
import {
  BlobServiceClient,
  StorageSharedKeyCredential,
  ContainerClient,
  RestError,
} from '@azure/storage-blob';
import { DefaultAzureCredential } from '@azure/identity';

import { BlobInfo, HashStorageAdapter } from '../../core/adapters';
import { AzureBlobStorageConfig } from '../../core/config';
import { RDIndex } from '../../core/models';

export class AzureBlobStorageAdapter extends HashStorageAdapter {
  private container: ContainerClient;
  private readonly prefix: string;

  constructor(private readonly config: AzureBlobStorageConfig) {
    super();

    const { endpoint, container, credentials, pathPrefix } = this.config;
    this.prefix = pathPrefix ? pathPrefix.replace(/\/$/, '') : '';

    let blobServiceClient: BlobServiceClient;

    if (credentials.accountKey && credentials.accountName) {
      const credential = new StorageSharedKeyCredential(
        credentials.accountName,
        credentials.accountKey
      );

      blobServiceClient = new BlobServiceClient(endpoint, credential);
    } else if (credentials.sasToken) {
      const sasUrl =
        endpoint.includes('?') || credentials.sasToken.startsWith('?')
          ? `${endpoint}${credentials.sasToken}`
          : `${endpoint}?${credentials.sasToken}`;

      blobServiceClient = new BlobServiceClient(sasUrl);
    } else {
      const credential = new DefaultAzureCredential();

      blobServiceClient = new BlobServiceClient(endpoint, credential);
    }

    this.container = blobServiceClient.getContainerClient(container);
  }

  async dispose(): Promise<void> {
    return;
  }

  private getChunkPath(hash: string) {
    return this.prefix ? `${this.prefix}/chunks/${hash}` : `chunks/${hash}`;
  }

  private getIndexPath() {
    return this.prefix ? `${this.prefix}/rd-index.json` : 'rd-index.json';
  }

  async getChunk(hash: string): Promise<Readable | null> {
    try {
      const blob = this.container.getBlobClient(this.getChunkPath(hash));

      if (!(await blob.exists())) {
        return null;
      }

      const download = await blob.download();
      return download.readableStreamBody as Readable;
    } catch (error) {
      if (error instanceof RestError && error.statusCode === 404) {
        return null;
      }

      throw error;
    }
  }

  async putChunk(hash: string, data: Readable, opts?: { overwrite?: boolean }): Promise<void> {
    try {
      const blob = this.container.getBlockBlobClient(this.getChunkPath(hash));

      if (!opts?.overwrite) {
        const exists = await blob.exists();

        if (exists) {
          return;
        }
      }

      await blob.uploadStream(data, 4 * 1024 * 1024, 5, {
        blobHTTPHeaders: { blobContentType: 'application/octet-stream' },
      });
    } catch (error) {
      throw error;
    }
  }

  async chunkExists(hash: string): Promise<boolean> {
    const blob = this.container.getBlobClient(this.getChunkPath(hash));
    return blob.exists();
  }

  async deleteChunk(hash: string): Promise<void> {
    const blob = this.container.getBlobClient(this.getChunkPath(hash));
    await blob.deleteIfExists();
  }

  async listChunks(): Promise<string[]> {
    const hashes: string[] = [];
    const chunksPath = this.prefix ? `${this.prefix}/chunks/` : 'chunks/';

    for await (const blob of this.container.listBlobsFlat({ prefix: chunksPath })) {
      const name = blob.name.startsWith(chunksPath)
        ? blob.name.slice(chunksPath.length)
        : blob.name;
      hashes.push(name);
    }

    return hashes;
  }

  async getChunkInfo(hash: string): Promise<BlobInfo | null> {
    try {
      const blob = this.container.getBlobClient(this.getChunkPath(hash));

      if (!(await blob.exists())) {
        return null;
      }

      const props = await blob.getProperties();

      return {
        hash,
        size: props.contentLength ?? 0,
        modified: props.lastModified ?? undefined,
        metadata: props.metadata ?? undefined,
      };
    } catch (error) {
      if (error instanceof RestError && error.statusCode === 404) {
        return null;
      }

      throw error;
    }
  }

  async getRemoteIndex(): Promise<RDIndex | null> {
    try {
      const blob = this.container.getBlobClient(this.getIndexPath());

      const exists = await blob.exists();
      if (!exists) {
        return null;
      }

      const download = await blob.download();
      const chunks: Buffer[] = [];

      const stream = download.readableStreamBody;
      if (!stream) {
        return null;
      }

      for await (const chunk of stream) {
        chunks.push(typeof chunk === 'string' ? Buffer.from(chunk, 'utf-8') : chunk);
      }

      const data = Buffer.concat(chunks).toString('utf-8');
      return JSON.parse(data) as RDIndex;
    } catch (error) {
      if (error instanceof RestError && error.statusCode === 404) {
        return null;
      }

      throw error;
    }
  }

  async putRemoteIndex(index: RDIndex): Promise<void> {
    const blob = this.container.getBlockBlobClient(this.getIndexPath());
    const contents = JSON.stringify(index, null, 2);

    await blob.upload(contents, Buffer.byteLength(contents), {
      blobHTTPHeaders: { blobContentType: 'application/json' },
    });
  }
}
