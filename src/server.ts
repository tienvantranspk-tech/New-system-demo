import express from 'express';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { pool } from './db';

const __dirname = dirname(fileURLToPath(import.meta.url));
const app = express();
app.use(express.json());

// Serve the web UI (web/index.html)
app.use(express.static(join(__dirname, '..', 'web')));

// Health check (mirrors a Cloud Run health endpoint)
app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'ok', db: 'up' });
  } catch (err) {
    res.status(500).json({ status: 'error', db: 'down', message: String(err) });
  }
});

// List items
app.get('/api/items', async (_req, res) => {
  const { rows } = await pool.query('SELECT * FROM items ORDER BY id');
  res.json(rows);
});

// Create item
app.post('/api/items', async (req, res) => {
  const { sku, name, quantity, unit_price } = req.body ?? {};
  if (!sku || !name) {
    return res.status(400).json({ error: 'sku and name are required' });
  }
  try {
    const { rows } = await pool.query(
      `INSERT INTO items (sku, name, quantity, unit_price)
       VALUES ($1, $2, $3, $4) RETURNING *`,
      [sku, name, Number(quantity) || 0, Number(unit_price) || 0],
    );
    res.status(201).json(rows[0]);
  } catch (err: any) {
    if (err?.code === '23505') {
      return res.status(409).json({ error: `SKU "${sku}" already exists` });
    }
    res.status(500).json({ error: String(err) });
  }
});

// Delete item
app.delete('/api/items/:id', async (req, res) => {
  const { rowCount } = await pool.query('DELETE FROM items WHERE id = $1', [req.params.id]);
  if (!rowCount) return res.status(404).json({ error: 'not found' });
  res.status(204).end();
});

// Update item
app.put('/api/items/:id', async (req, res) => {
  const { id } = req.params;
  const { sku, name, quantity, unit_price } = req.body ?? {};
  if (!sku || !name) {
    return res.status(400).json({ error: 'sku and name are required' });
  }
  try {
    const { rows, rowCount } = await pool.query(
      `UPDATE items
       SET sku = $1, name = $2, quantity = $3, unit_price = $4
       WHERE id = $5 RETURNING *`,
      [sku, name, Number(quantity) || 0, Number(unit_price) || 0, id],
    );
    if (!rowCount) return res.status(404).json({ error: 'not found' });
    res.json(rows[0]);
  } catch (err: any) {
    if (err?.code === '23505') {
      return res.status(409).json({ error: `SKU "${sku}" already exists` });
    }
    res.status(500).json({ error: String(err) });
  }
});

const port = Number(process.env.PORT) || 8080;
app.listen(port, () => {
  console.log(`ERP sample running at http://localhost:${port}`);
});
