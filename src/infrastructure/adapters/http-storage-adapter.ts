import { Readable } from 'stream';

import { BlobInfo, HashStorageAdapter } from '../../core/adapters';
import { Nullish, UnknownAny } from '../../core/types';
import { HTTPStorageConfig } from '../../core/config';
import { RDIndex } from '../../core/models';
import {
  DeleteChunkException,
  GetChunkException,
  GetRemoteIndexException,
  HeadChunkException,
  PutChunkException,
  PutRemoteIndexException,
} from '../../core/exceptions';

export class HTTPStorageAdapter extends HashStorageAdapter {
  private readonly baseUrl: string;
  private readonly token?: Nullish<string>;
  private readonly apiKey?: Nullish<string>;

  constructor(private readonly config: HTTPStorageConfig) {
    super();

    const { endpoint, credentials } = config;

    this.baseUrl = endpoint.replace(/\/+$/, '');
    this.token = credentials?.bearerToken;
    this.apiKey = credentials?.apiKey;
  }

  async dispose(): Promise<void> {
    return;
  }

  private buildUrl(hash: string): string {
    const prefix = this.config.pathPrefix
      ? `/${this.config.pathPrefix.replace(/^\/+|\/+$/g, '')}`
      : '';

    return `${this.baseUrl}${prefix}/${encodeURIComponent(hash)}`;
  }

  private buildIndexUrl(): string {
    const prefix = this.config.pathPrefix
      ? `/${this.config.pathPrefix.replace(/^\/+|\/+$/g, '')}`
      : '';

    const indexFile = this.config.indexFilePath ?? 'rd-index.json';

    return `${this.baseUrl}${prefix ? prefix + '/' : '/'}${indexFile}`;
  }

  private headers(extra?: Nullish<Record<string, string>>): Record<string, string> {
    const headers: Record<string, string> = { ...(extra || {}) };

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    if (this.apiKey) {
      headers['x-api-key'] = this.apiKey;
    }

    return headers;
  }

  async getChunk(hash: string): Promise<Readable | null> {
    const url = this.buildUrl(hash);

    const res = await fetch(url, {
      method: 'GET',
      headers: this.headers(),
    });

    if (res.status === 404) {
      return null;
    }

    if (!res.ok) {
      throw new GetChunkException(`${res.status} ${res.statusText}`);
    }

    // Convert web ReadableStream to Node.js Readable
    const nodeStream = Readable.fromWeb(res.body as UnknownAny);
    return nodeStream;
  }

  async putChunk(hash: string, data: Readable): Promise<void> {
    const url = this.buildUrl(hash);

    const res = await fetch(url, {
      method: 'PUT',
      headers: this.headers({ 'Content-Type': 'application/octet-stream' }),
      body: data as UnknownAny,
    });

    if (!res.ok) {
      throw new PutChunkException(`${res.status} ${res.statusText}`);
    }
  }

  async chunkExists(hash: string): Promise<boolean> {
    const url = this.buildUrl(hash);

    const res = await fetch(url, {
      method: 'HEAD',
      headers: this.headers(),
    });

    if (res.status === 404) {
      return false;
    }

    if (!res.ok && res.status !== 200) {
      throw new HeadChunkException(`${res.status} ${res.statusText}`);
    }

    return res.ok;
  }

  async deleteChunk(hash: string): Promise<void> {
    const url = this.buildUrl(hash);

    const res = await fetch(url, {
      method: 'DELETE',
      headers: this.headers(),
    });

    if (res.status === 404) {
      return;
    }

    if (!res.ok) {
      throw new DeleteChunkException(`${res.status} ${res.statusText}`);
    }
  }

  async getChunkInfo(hash: string): Promise<BlobInfo | null> {
    const url = this.buildUrl(hash);

    const res = await fetch(url, {
      method: 'HEAD',
      headers: this.headers(),
    });

    if (res.status === 404) {
      return null;
    }

    if (!res.ok) {
      throw new HeadChunkException(`${res.status} ${res.statusText}`);
    }

    const size = Number(res.headers.get('content-length')) || 0;
    const modified = res.headers.get('last-modified')
      ? new Date(res.headers.get('last-modified')!)
      : undefined;

    const metadata: Record<string, string> = {};

    for (const [key, value] of res.headers.entries()) {
      if (key.startsWith('x-meta-')) {
        metadata[key.substring(7)] = value;
      }
    }

    return { hash, size, modified, metadata };
  }

  async getRemoteIndex(): Promise<RDIndex | null> {
    const url = this.buildIndexUrl();

    const res = await fetch(url, {
      method: 'GET',
      headers: this.headers(),
    });

    if (res.status === 404) {
      return null;
    }

    if (!res.ok) {
      throw new GetRemoteIndexException(`${res.status} ${res.statusText}`);
    }

    const buffer = Buffer.from(await res.arrayBuffer());
    const data = buffer.toString('utf-8');

    return JSON.parse(data) as RDIndex;
  }

  async putRemoteIndex(index: RDIndex): Promise<void> {
    const url = this.buildIndexUrl();

    const res = await fetch(url, {
      method: 'PUT',
      headers: this.headers({
        'Content-Type': 'application/json',
      }),
      body: JSON.stringify(index, null, 2),
    });

    if (!res.ok) {
      throw new PutRemoteIndexException(`${res.status} ${res.statusText}`);
    }
  }
}
