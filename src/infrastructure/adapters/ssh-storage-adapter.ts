import { SFTPWrapper, Client } from 'ssh2';
import { Readable } from 'stream';
import { join } from 'path';

import { BlobInfo, HashStorageAdapter } from '../../core/adapters';
import { Nullish, UnknownAny } from '../../core/types';
import { SSHStorageConfig } from '../../core/config';
import { RDIndex } from '../../core/models';

type SFTPError = Error & { code?: number };

export class SSHStorageAdapter extends HashStorageAdapter {
  private sftp: SFTPWrapper | null = null;
  private client: Client | null = null;

  constructor(private readonly config: SSHStorageConfig) {
    super();
  }

  private connecting: Promise<SFTPWrapper> | null = null;

  private async connect(): Promise<SFTPWrapper> {
    if (this.sftp) {
      return this.sftp;
    }

    if (this.connecting) {
      return this.connecting;
    }

    this.connecting = new Promise<SFTPWrapper>((resolve, reject) => {
      const client = new Client();
      client
        .on('ready', () => {
          client.sftp((err, sftp) => {
            if (err) {
              client.end();
              this.connecting = null;
              return reject(err);
            }

            this.client = client;
            this.sftp = sftp;
            this.connecting = null;
            resolve(sftp);
          });
        })
        .on('error', (err) => {
          this.connecting = null;
          reject(err);
        })
        .connect({
          host: this.config.host,
          port: this.config.port ?? 22,
          username: this.config.credentials.username,
          password: this.config.credentials.password,
          privateKey: this.config.credentials.privateKey,
        });
    });

    return this.connecting;
  }

  private resolveChunkPath(hash: string): string {
    const prefix = this.config.pathPrefix ?? '';
    return join(prefix, 'chunks', hash);
  }

  private resolveIndexPath(): string {
    const prefix = this.config.pathPrefix ?? '';
    return join(prefix, 'rd-index.json');
  }

  async getChunk(hash: string): Promise<Readable | null> {
    const sftp = await this.connect();
    const remotePath = this.resolveChunkPath(hash);

    return new Promise((resolve, reject) => {
      sftp.stat(remotePath, (error: Nullish<SFTPError>) => {
        if (error) {
          if (error?.code === 2) {
            return resolve(null); // ENOENT
          }

          return reject(error);
        }

        try {
          const stream = sftp.createReadStream(remotePath);
          resolve(stream);
        } catch (err) {
          reject(err);
        }
      });
    });
  }

  async putChunk(hash: string, data: Readable): Promise<void> {
    const sftp = await this.connect();
    const remotePath = this.resolveChunkPath(hash);
    const dir = remotePath.substring(0, remotePath.lastIndexOf('/'));

    await this.createDirIfNotFound(sftp, dir);

    return new Promise((resolve, reject) => {
      const writeStream = sftp.createWriteStream(remotePath);

      data.pipe(writeStream);
      writeStream.on('close', resolve);
      writeStream.on('error', reject);
    });
  }

  async chunkExists(hash: string): Promise<boolean> {
    const sftp = await this.connect();
    const remotePath = this.resolveChunkPath(hash);

    return new Promise((resolve) => {
      sftp.stat(remotePath, (error) => resolve(error ? false : true));
    });
  }

  async deleteChunk(hash: string): Promise<void> {
    const sftp = await this.connect();
    const remotePath = this.resolveChunkPath(hash);

    return new Promise((resolve, reject) => {
      sftp.unlink(remotePath, (error: Nullish<SFTPError> | null) => {
        if (error && error?.code !== 2) {
          return reject(error);
        }

        resolve();
      });
    });
  }

  async listChunks(): Promise<string[]> {
    const sftp = await this.connect();
    const prefix = join(this.config.pathPrefix ?? '', 'chunks');

    return new Promise((resolve, reject) => {
      sftp.readdir(prefix, (error, list) => {
        if (error) {
          return reject(error);
        }

        resolve(list.map((file) => file.filename));
      });
    });
  }

  async getChunkInfo(hash: string): Promise<BlobInfo | null> {
    const sftp = await this.connect();
    const remotePath = this.resolveChunkPath(hash);

    return new Promise((resolve, reject) => {
      sftp.stat(remotePath, (error: Nullish<SFTPError>, stats) => {
        if (error) {
          if (error.code === 2) {
            return resolve(null);
          }

          return reject(error);
        }

        resolve({
          hash,
          size: stats.size,
          modified: stats.mtime ? new Date(stats.mtime * 1000) : undefined,
        });
      });
    });
  }

  async getRemoteIndex(): Promise<RDIndex | null> {
    const sftp = await this.connect();
    const remotePath = this.resolveIndexPath();

    return new Promise((resolve, reject) => {
      sftp.stat(remotePath, (error: Nullish<SFTPError>) => {
        if (error) {
          if (error.code === 2) {
            return resolve(null);
          }

          return reject(error);
        }

        try {
          const stream = sftp.createReadStream(remotePath);

          this.readStream(stream)
            .then((data) => {
              try {
                const index = JSON.parse(data) as RDIndex;
                resolve(index);
              } catch (err) {
                reject(err);
              }
            })
            .catch(reject);
        } catch (err) {
          reject(err);
        }
      });
    });
  }

  private readStream(stream: Readable): Promise<string> {
    return new Promise((resolve, reject) => {
      let data = '';

      stream.on('data', (chunk) => {
        data += chunk.toString();
      });

      stream.on('end', () => resolve(data));
      stream.on('error', reject);
    });
  }

  async putRemoteIndex(index: RDIndex): Promise<void> {
    const sftp = await this.connect();
    const remotePath = this.resolveIndexPath();
    const json = JSON.stringify(index, null, 2);

    return new Promise((resolve, reject) => {
      const writeStream = sftp.createWriteStream(remotePath, { flags: 'w' });
      const readable = Readable.from([json]);

      writeStream.on('close', resolve);
      writeStream.on('error', reject);

      readable.pipe(writeStream);
    });
  }

  async dispose(): Promise<void> {
    if (this.sftp && typeof this.sftp.end === 'function') {
      this.sftp.end();
    }
    this.sftp = null;

    if (this.client !== null) {
      await new Promise<void>((resolve) => {
        this.client?.once('close', resolve);
        this.client?.end();
      });
      this.client = null;
    }
  }

  private async createDirIfNotFound(sftp: SFTPWrapper, dir: string): Promise<void> {
    const parts = dir.split('/').filter(Boolean);
    let current = '';

    for (const part of parts) {
      current += '/' + part;
      try {
        await this.promisifyStat(sftp, current);
      } catch (error: UnknownAny) {
        if (error?.code === 2) {
          await this.promisifyMkdir(sftp, current);
        }
      }
    }
  }

  private promisifyStat(sftp: SFTPWrapper, path: string) {
    return new Promise((resolve, reject) => {
      sftp.stat(path, (err, stats) => (err ? reject(err) : resolve(stats)));
    });
  }

  private promisifyMkdir(sftp: SFTPWrapper, path: string): Promise<void> {
    return new Promise((resolve, reject) => {
      sftp.mkdir(path, (err) => (err ? reject(err) : resolve()));
    });
  }
}
