/**
 * Returns a string that does not conflict with existing values by adding suffix to it.
 */
export function addUniqueSuffix(
  prefix: string,
  existing: string[],
  sep: string = '_',
): string {
  const set = new Set(existing);
  if (!set.has(prefix)) {
    return prefix;
  }
  for (let i = 1; i < existing.length; ++i) {
    const candidate = `${prefix}${sep}${i}`;
    if (!set.has(candidate)) {
      return candidate;
    }
  }
  return `${prefix}${sep}${existing.length + 1}`;
}
