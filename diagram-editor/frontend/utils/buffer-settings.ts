import type { RetentionPolicy } from '../types/api';

export type RetentionMode = 'keep_last' | 'keep_first' | 'keep_all';

export function getRetentionMode(retention: RetentionPolicy): RetentionMode {
  return typeof retention === 'string'
    ? 'keep_all'
    : (Object.keys(retention)[0] as RetentionMode);
}
