export function invariant(
  message: string,
  condition: unknown,
  exception?: Error
): asserts condition {
  if (!condition && exception) {
    throw exception;
  }

  if (!condition && !exception) {
    throw new Error(message);
  }
}
