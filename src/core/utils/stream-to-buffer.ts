import { Readable } from 'stream';

export async function streamToBuffer(stream: Readable): Promise<Buffer> {
  const buffers: Buffer[] = [];

  for await (const chunk of stream) {
    buffers.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(buffers);
}
