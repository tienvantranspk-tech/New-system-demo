import 'dotenv/config';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

const __dirname = dirname(fileURLToPath(import.meta.url));
const url = new URL(process.env.DATABASE_URL);
const dbName = url.pathname.slice(1);

// 1) Ensure the database exists (connect to the default 'postgres' db first)
const admin = new pg.Client({
  host: url.hostname,
  port: url.port || 5432,
  user: decodeURIComponent(url.username || 'postgres'),
  password: url.password ? decodeURIComponent(url.password) : undefined,
  database: 'postgres',
});
await admin.connect();
const exists = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [dbName]);
if (exists.rowCount === 0) {
  await admin.query(`CREATE DATABASE "${dbName}"`);
  console.log(`Created database "${dbName}"`);
} else {
  console.log(`Database "${dbName}" already exists`);
}
await admin.end();

// 2) Apply schema + seed to our database
const client = new pg.Client({ connectionString: process.env.DATABASE_URL });
await client.connect();
const sql = await readFile(join(__dirname, '..', 'db', 'schema.sql'), 'utf8');
await client.query(sql);
await client.end();
console.log('Schema applied + seed inserted.');
