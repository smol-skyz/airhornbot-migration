import 'dotenv/config';
import { Redis } from 'ioredis';
import pg from 'pg';
import { getAllKeysScan, getCountForKey } from './utils';
import { stringify } from 'csv-stringify';
import { writeFileSync, createReadStream } from 'fs';
import { createInterface } from 'readline';

const { REDIS_PREFIX, EXPORT_CSV, IMPORT_CSV, CSV_PATH } = process.env;

const redis = new Redis({
  host: process.env.REDIS_HOST!,
  port: parseInt(process.env.REDIS_PORT!, 10),
  password: process.env.REDIS_PASSWORD,
});

const postgres = new pg.Client({
  host: process.env.POSTGRES_HOST,
  port: parseInt(process.env.POSTGRES_PORT!, 10),
  user: process.env.POSTGRES_USER!,
  password: process.env.POSTGRES_PASSWORD,
  database: process.env.POSTGRES_DATABASE!,
});

async function exportCsv(): Promise<unknown> {
  const csvRows: string[] = [];

  const keys = await getAllKeysScan(redis, `${REDIS_PREFIX}:counts:*`);
  keys.push(`${REDIS_PREFIX}:total`);

  const stringifier = stringify({
    delimiter: ',',
    quote: true,
  });

  stringifier.on('readable', () => {
    let row;
    while ((row = stringifier.read()) !== null) {
      csvRows.push(row);
    }
  });

  stringifier.on('error', (err) => {
    console.error(err.message);
  });

  stringifier.on('finish', () => {
    writeFileSync(CSV_PATH!, csvRows.join(''));
    console.log('Exporting completed.');
  });

  for (let key of keys) {
    const count = await getCountForKey(redis, key);
    stringifier.write([key, count]);
    console.log(`${key} - ${count}`);
  }

  stringifier.end();
  return;
}

async function importCsv(): Promise<unknown> {
  const soundNames = new Set<string>();
  const rows = [];

  const fileStream = createReadStream(CSV_PATH!);
  const readline = createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of readline) {
    const splitLine = line.split(',');
    if (splitLine.length < 2) {
      continue;
    }
    const key = splitLine[0];
    const value = splitLine[1];
    const matchedData = new RegExp(
      `${REDIS_PREFIX}:counts:guild:(?<userId>\\d+):channel:(?<channelId>\\d+):sound:(?<soundName>[\\w_-]+)`,
      'g'
    ).exec(key);
    if (matchedData === null) {
      continue;
    }
    rows.push({
      userId: matchedData.groups!.userId,
      channelId: matchedData.groups!.channelId,
      soundName: matchedData.groups!.soundName,
      counter: parseInt(value, 10),
    });
    soundNames.add(matchedData.groups!.soundName);
  }

  console.log(`Rows to import: ${rows.length}`);

  const soundCommandMap = new Map<string, number>();
  const soundMap = new Map<string, number>();

  for (let soundName of soundNames) {
    // Insert the sound command
    const soundCommandResult = await postgres.query<{
      id: number;
    }>(
      `INSERT INTO public."SoundCommand"("name", "prettyName", "description", "disabled") VALUES ($1, $1, 'Imported sound', true) RETURNING *;`,
      [soundName]
    );
    soundCommandMap.set(soundName, soundCommandResult.rows[0].id);
    // Insert the sound
    const soundResult = await postgres.query<{
      id: number;
    }>(
      `INSERT INTO public."Sound"("soundCommandId", "name", "fileReference", "disabled") VALUES ($1, 'Legacy', './sounds/airhorn/airhorn_default.ogg', true) RETURNING *;`,
      [soundCommandMap.get(soundName)!]
    );
    soundMap.set(soundName, soundResult.rows[0].id);
  }

  // Insert the rows for the usages
  for (let row of rows) {
    await postgres.query(
      `INSERT INTO public."User"("id", "username", "discriminator", "lastUpdate") VALUES ($1, 'Imported User', '0000', 1) ON CONFLICT DO NOTHING;`,
      [BigInt(row.userId)]
    );
    await postgres.query(`INSERT INTO public."Usage"("guildId", "channelId", "userId", "soundId", counter) VALUES ($1, $2, $3, $4, $5);`, [
      BigInt(1), // The guild id in the v2 was not stored correctly, so a read repair has been implemented into v3
      BigInt(row.channelId),
      BigInt(row.userId),
      soundMap.get(row.soundName)!,
      row.counter,
    ]);
  }

  console.log('Importing completed.');

  return;
}

async function main() {
  await postgres.connect();
  if (EXPORT_CSV === 'false') {
    await exportCsv();
  }
  if (IMPORT_CSV === 'true') {
    await importCsv();
  }
  console.log('Done.');
}

main().catch((e) => {
  throw e;
});
