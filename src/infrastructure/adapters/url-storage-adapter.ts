import { Readable } from 'stream';

import { BlobInfo, UrlStorageAdapter } from '../../core/adapters';
import {
  DeleteChunkException,
  GetChunkException,
  GetRemoteIndexException,
  HeadChunkException,
  PutChunkException,
  PutRemoteIndexException,
} from '../../core/exceptions';
import { UnknownAny } from '../../core/types';
import { RDIndex } from '../../core/models';

export class DefaultUrlStorageAdapter extends UrlStorageAdapter {
  async dispose(): Promise<void> {
    return;
  }

  async getChunkByUrl(url: string): Promise<Readable | null> {
    const res = await fetch(url, { method: 'GET', headers: {} });

    if (res.status === 404) {
      return null;
    }

    if (!res.ok) {
      throw new GetChunkException(`${res.status} ${res.statusText}`);
    }

    return Readable.fromWeb(res.body as UnknownAny);
  }

  async putChunkByUrl(url: string, data: Readable): Promise<void> {
    const res = await fetch(url, {
      method: 'PUT',
      headers: { ...{}, 'Content-Type': 'application/octet-stream' },
      body: data as UnknownAny,
    });

    if (!res.ok) {
      throw new PutChunkException(`${res.status} ${res.statusText}`);
    }
  }

  async chunkExistsByUrl(url: string): Promise<boolean> {
    const res = await fetch(url, { method: 'HEAD', headers: {} });

    if (res.status === 404) {
      return false;
    }

    if (!res.ok && res.status !== 200) {
      throw new HeadChunkException(`${res.status} ${res.statusText}`);
    }

    return res.ok;
  }

  async deleteChunkByUrl(url: string): Promise<void> {
    const res = await fetch(url, { method: 'DELETE', headers: {} });

    if (res.status === 404) {
      return;
    }

    if (!res.ok) {
      throw new DeleteChunkException(`${res.status} ${res.statusText}`);
    }
  }

  async getChunkInfoByUrl(hash: string, url: string): Promise<BlobInfo | null> {
    const res = await fetch(url, { method: 'HEAD', headers: {} });

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

    for (const [k, v] of res.headers.entries()) {
      if (k.startsWith('x-meta-')) metadata[k.substring(7)] = v;
    }

    return { hash, size, modified, metadata };
  }

  async getRemoteIndexByUrl(url: string): Promise<RDIndex | null> {
    const res = await fetch(url, { method: 'GET', headers: {} });

    if (res.status === 404) {
      return null;
    }

    if (!res.ok) {
      throw new GetRemoteIndexException(`${res.status} ${res.statusText}`);
    }

    return (await res.json()) as RDIndex;
  }

  async putRemoteIndexByUrl(url: string, index: RDIndex): Promise<void> {
    const res = await fetch(url, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(index, null, 2),
    });

    if (!res.ok) {
      throw new PutRemoteIndexException(`${res.status} ${res.statusText}`);
    }
  }
}
