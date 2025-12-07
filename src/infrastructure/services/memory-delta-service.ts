import { join, isAbsolute, resolve } from 'path';
import { stat, readdir } from 'fs/promises';

import { AsyncChunkStream, DeltaService, HasherService } from '../../core/services';
import { RDIndex, DeltaPlan, FileEntry, ChunkEntry } from '../../core/models';
import { invariant } from '../../core/utils/invariant';
import { Nullish } from '../../core/types';

export class MemoryDeltaService implements DeltaService {
  constructor(private readonly hasher: HasherService) {}

  async createIndexFromDirectory(
    rootPath: string,
    chunkSize: number,
    concurrency = 6,
    ignorePatterns?: Nullish<string[]>
  ): Promise<RDIndex> {
    invariant('rootPath must be a valid string', typeof rootPath === 'string' && rootPath !== '');

    const rootDir = isAbsolute(rootPath) ? rootPath : resolve(process.cwd(), rootPath);

    const filePaths: string[] = [];
    for await (const relativePath of this.walkFiles(rootDir, undefined, ignorePatterns)) {
      filePaths.push(relativePath.replace(/\\/g, '/'));
    }

    const ignoredFiles = new Set(['rd-index.json']);
    const filteredPaths = filePaths.filter((p) => !ignoredFiles.has(p));

    const results: FileEntry[] = new Array(filteredPaths.length);
    let currentIndex = 0;

    const worker = async (): Promise<void> => {
      while (true) {
        const i = currentIndex++;
        if (i >= filteredPaths.length) {
          break;
        }

        const relativePath = filteredPaths[i];
        const fullPath = join(rootDir, relativePath);
        const stats = await stat(fullPath);
        const fileEntry = await this.hasher.hashFile(relativePath, rootDir, chunkSize);

        results[i] = {
          ...fileEntry,
          path: relativePath,
          modifiedAt: stats.mtimeMs,
        };
      }
    };

    await Promise.all(Array.from({ length: concurrency }, () => worker()));

    return {
      version: 1,
      createdAt: Date.now(),
      chunkSize,
      files: results,
    };
  }

  async createFileEntryFromStream(stream: AsyncChunkStream, path: string): Promise<FileEntry> {
    const fileHasher = await this.hasher.createStreamingHasher();

    const chunks = await this.hasher.hashStream(stream, (chunk) => {
      fileHasher.update(chunk);
    });

    const fileHash = fileHasher.digest('hex');

    const totalSize = chunks.reduce((total, chunk) => total + chunk.size, 0);
    const fileEntry: FileEntry = {
      path: path.replace(/\\/g, '/'),
      size: totalSize,
      hash: fileHash,
      modifiedAt: Date.now(),
      chunks,
    };

    return fileEntry;
  }

  private async *walkFiles(
    dir: string,
    prefix = '',
    ignorePatterns?: Nullish<string[]>
  ): AsyncGenerator<string> {
    const entries = await readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      const absPath = join(dir, entry.name);
      const relPath = join(prefix, entry.name).replace(/\\/g, '/');

      if (ignorePatterns?.length && this.matchesAnyPattern(relPath, ignorePatterns)) {
        continue;
      }

      if (entry.isFile()) {
        yield relPath;
      } else if (entry.isDirectory()) {
        yield* this.walkFiles(absPath, relPath, ignorePatterns);
      }
    }
  }

  private matchesAnyPattern(path: string, patterns: string[]): boolean {
    for (const pattern of patterns) {
      const regex = this.globToRegex(pattern);

      if (regex.test(path)) {
        return true;
      }
    }

    return false;
  }

  private globToRegex(glob: string): RegExp {
    glob = glob.replace(/\\/g, '/');

    let pattern = glob.replace(/([.+^${}()|[\]\\])/g, '\\$1');
    pattern = pattern.replace(/\*\*\/?/g, '(.*/)?');
    pattern = pattern.replace(/\*/g, '[^/]*');
    pattern = '^' + pattern;

    return new RegExp(pattern, 'i');
  }

  // source: where the changes come
  // target: where the changes will apply
  compare(source: RDIndex, target: RDIndex | null): DeltaPlan {
    const deltaPlan: DeltaPlan = {
      deletedFiles: [],
      missingChunks: [],
      obsoleteChunks: [],
      newAndModifiedFiles: [],
    };

    const targetFilesMap = new Map<string, FileEntry>();
    const sourcePaths = new Set(source.files.map((f) => f.path));

    if (target) {
      for (const file of target.files) {
        targetFilesMap.set(file.path, file);

        // Check for deleted files (they exist in target but not in source, so they will be removed from target)
        if (!sourcePaths.has(file.path)) {
          deltaPlan.deletedFiles.push(file.path);

          for (const chunk of file.chunks) {
            deltaPlan.obsoleteChunks.push({ ...chunk, filePath: file.path });
          }
        }
      }
    }

    // Process source files
    for (const srcFile of source.files) {
      const targetFile = targetFilesMap.get(srcFile.path);

      // New file (not in target)
      if (!targetFile) {
        deltaPlan.missingChunks.push(
          ...srcFile.chunks.map((c) => ({ ...c, filePath: srcFile.path }))
        );
        deltaPlan.newAndModifiedFiles.push(srcFile);
        continue;
      }

      const targetChunks = new Map(targetFile.chunks.map((c) => [`${c.hash}@${c.offset}`, c]));
      const sourceChunkKeys = new Set(srcFile.chunks.map((c) => `${c.hash}@${c.offset}`));

      // File exists -> compare chunks
      for (const chunk of srcFile.chunks) {
        const key = `${chunk.hash}@${chunk.offset}`;

        if (!targetChunks.has(key)) {
          deltaPlan.missingChunks.push({ ...chunk, filePath: srcFile.path });
          deltaPlan.newAndModifiedFiles.push(srcFile);
        }
      }

      // Check obsolete chunks in target that are not in source
      for (const chunk of targetFile.chunks) {
        const key = `${chunk.hash}@${chunk.offset}`;

        if (!sourceChunkKeys.has(key)) {
          deltaPlan.obsoleteChunks.push({ ...chunk, filePath: srcFile.path });
        }
      }
    }

    return deltaPlan;
  }

  mergePlans(base: DeltaPlan, updates: DeltaPlan): DeltaPlan {
    const mergeFiles = (a: FileEntry[], b: FileEntry[]): FileEntry[] => {
      const seen = new Set<string>();
      const result: FileEntry[] = [];

      for (const file of [...a, ...b]) {
        if (!seen.has(file.path)) {
          seen.add(file.path);
          result.push(file);
        }
      }

      return result;
    };

    const mergeChunks = (a: ChunkEntry[], b: ChunkEntry[]): ChunkEntry[] => {
      const seen = new Set<string>();
      const result: ChunkEntry[] = [];

      for (const chunk of [...a, ...b]) {
        const key = `${chunk.hash}@${chunk.offset}`;
        if (!seen.has(key)) {
          seen.add(key);
          result.push(chunk);
        }
      }

      return result;
    };

    const mergeStrings = (a: string[], b: string[]): string[] => {
      return Array.from(new Set([...a, ...b]));
    };

    return {
      deletedFiles: mergeStrings(base.deletedFiles, updates.deletedFiles),
      missingChunks: mergeChunks(base.missingChunks, updates.missingChunks),
      obsoleteChunks: mergeChunks(base.obsoleteChunks, updates.obsoleteChunks),
      newAndModifiedFiles: mergeFiles(base.newAndModifiedFiles, updates.newAndModifiedFiles),
    };
  }

  async compareForUpload(localIndex: RDIndex, remoteIndex: RDIndex | null): Promise<DeltaPlan> {
    const deltaPlan = this.compare(localIndex, remoteIndex);

    const allNeededHashes = new Set<string>();

    for (const file of localIndex.files) {
      for (const chunk of file.chunks) {
        allNeededHashes.add(chunk.hash);
      }
    }

    deltaPlan.obsoleteChunks = deltaPlan.obsoleteChunks.filter(
      (chunk) => !allNeededHashes.has(chunk.hash)
    );

    return deltaPlan;
  }

  async compareForDownload(localIndex: RDIndex | null, remoteIndex: RDIndex): Promise<DeltaPlan> {
    return this.compare(remoteIndex, localIndex);
  }
}
