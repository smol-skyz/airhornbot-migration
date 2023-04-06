import { Redis } from 'ioredis';

export async function getAllKeysScan(redis: Redis, pattern: string): Promise<string[]> {
  const stream = redis.scanStream({
    match: pattern,
  });
  const foundKeys: Set<string> = new Set<string>();
  stream.on('data', (resultKeys) => {
    for (let i = 0; i < resultKeys.length; i++) {
      if (foundKeys.has(resultKeys[i])) {
        continue;
      }
      foundKeys.add(resultKeys[i]);
    }
  });
  await new Promise<void>((resolve) => {
    stream.on('end', () => {
      resolve();
    });
  });
  return [...foundKeys.values()];
}

export async function getCountForKey(redis: Redis, key: string): Promise<number> {
  const result = await redis.get(key);
  if (!result) {
    return 0;
  }
  return Number(result);
}
