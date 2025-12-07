import { Readable } from 'stream';
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  NoSuchKey,
} from '@aws-sdk/client-s3';

import { BlobInfo, HashStorageAdapter } from '../../core/adapters';
import { S3StorageConfig } from '../../core/config';
import { RDIndex } from '../../core/models';
import { Nullish } from '../../core/types';

export class S3StorageAdapter extends HashStorageAdapter {
  private readonly s3: S3Client;

  constructor(private readonly config: S3StorageConfig) {
    super();

    this.s3 = new S3Client({
      region: config.region,
      endpoint: config.endpoint,
      credentials: config.credentials,
    });
  }

  async dispose(): Promise<void> {
    this.s3.destroy();
  }

  private resolveKey(hash: string): string {
    return this.config.pathPrefix ? `${this.config.pathPrefix}/chunks/${hash}` : hash;
  }

  async getChunk(hash: string): Promise<Readable | null> {
    try {
      const key = this.resolveKey(hash);
      const res = await this.s3.send(
        new GetObjectCommand({ Bucket: this.config.bucket, Key: key })
      );

      return res.Body as Readable;
    } catch (error) {
      if (error instanceof NoSuchKey) {
        return null;
      }

      throw error;
    }
  }

  async putChunk(
    hash: string,
    data: Readable,
    opts?: { overwrite?: Nullish<boolean>; size?: Nullish<number> }
  ): Promise<void> {
    const key = this.resolveKey(hash);

    if (!opts?.overwrite) {
      const exists = await this.chunkExists(hash);
      if (exists) {
        return;
      }
    }

    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.config.bucket,
        Key: key,
        Body: data,
        ContentLength: opts?.size,
      })
    );
  }

  async chunkExists(hash: string): Promise<boolean> {
    try {
      const key = this.resolveKey(hash);

      await this.s3.send(new HeadObjectCommand({ Bucket: this.config.bucket, Key: key }));

      return true;
    } catch (error) {
      return false;
    }
  }

  async deleteChunk(hash: string): Promise<void> {
    const key = this.resolveKey(hash);
    await this.s3.send(new DeleteObjectCommand({ Bucket: this.config.bucket, Key: key }));
  }

  async listChunks(): Promise<string[]> {
    const chunks: string[] = [];
    let continuationToken: Nullish<string>;
    const prefix = this.config.pathPrefix ? `${this.config.pathPrefix}/chunks` : undefined;

    do {
      const res = await this.s3.send(
        new ListObjectsV2Command({
          Bucket: this.config.bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        })
      );

      const keys = res.Contents?.map((obj) =>
        this.config.pathPrefix
          ? obj.Key!.replace(`${this.config.pathPrefix}/chunks/`, '')
          : obj.Key!
      );

      if (keys) {
        chunks.push(...keys);
      }

      continuationToken = res.NextContinuationToken;
    } while (continuationToken);

    return chunks;
  }

  async getChunkInfo(hash: string): Promise<BlobInfo | null> {
    try {
      const key = this.resolveKey(hash);
      const res = await this.s3.send(
        new HeadObjectCommand({ Bucket: this.config.bucket, Key: key })
      );
      return {
        hash,
        size: res.ContentLength ?? 0,
        modified: res.LastModified,
        metadata: res.Metadata,
      };
    } catch (error) {
      if (error instanceof NoSuchKey) {
        return null;
      }

      throw error;
    }
  }

  async putRemoteIndex(index: RDIndex): Promise<void> {
    const key = this.config.pathPrefix
      ? `${this.config.pathPrefix}/rd-index.json`
      : 'rd-index.json';

    await this.s3.send(
      new PutObjectCommand({
        Bucket: this.config.bucket,
        Key: key,
        Body: JSON.stringify(index),
        ContentType: 'application/json',
      })
    );
  }

  async getRemoteIndex(): Promise<RDIndex | null> {
    const key = this.config.pathPrefix
      ? `${this.config.pathPrefix}/rd-index.json`
      : 'rd-index.json';

    try {
      const result = await this.s3.send(
        new GetObjectCommand({
          Bucket: this.config.bucket,
          Key: key,
        })
      );

      if (!result.Body) {
        return null;
      }

      const buffer = Buffer.from(await result.Body.transformToByteArray());
      const data = buffer.toString('utf-8');

      return JSON.parse(data) as RDIndex;
    } catch (error) {
      if (error instanceof NoSuchKey) {
        return null;
      }

      throw error;
    }
  }
}
