import { Nullish, UnknownAny } from '../types';

/**
 * Throws an Abort exception if the given signal has been aborted.
 * Use at the start of async operations and between loop iterations for cooperative
 * cancellation via AbortController.
 */
export function checkAbort(signal?: Nullish<AbortSignal>): void {
  signal?.throwIfAborted();
}

/**
 * Returns true if the given error is an AbortError - i.e. the download was
 * intentionally cancelled by the user, not a real failure.
 */
export function isAbortError(err: UnknownAny): boolean {
  return err?.name === 'AbortError';
}
