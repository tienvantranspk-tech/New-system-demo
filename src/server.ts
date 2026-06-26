import express from 'express';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { pool } from './db';
import { eventBus } from './eventBus';
import { OutboxWorker } from './outboxWorker';

const __dirname = dirname(fileURLToPath(import.meta.url));
const app = express();
app.use(express.json());

// Global state to simulate FAST system status (Online / Offline)
let fastDowntime = false;

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

// List events (Pub/Sub monitor stream)
app.get('/api/events', async (_req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM audit_events ORDER BY id DESC LIMIT 15');
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

// List outbox events (FAST Sync stream)
app.get('/api/outbox', async (_req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM outbox_events ORDER BY id DESC LIMIT 15');
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

// GET current FAST mock downtime status
app.get('/api/fast-accounting/status', (_req, res) => {
  res.json({ fastDowntime });
});

// POST toggle FAST mock downtime (Offline / Online)
app.post('/api/fast-accounting/toggle-downtime', (req, res) => {
  const { downtime } = req.body ?? {};
  fastDowntime = !!downtime;
  console.log(`[FAST Mock API] Simulated downtime set to: ${fastDowntime}`);
  res.json({ fastDowntime });
});

// POST simulated FAST API endpoint to sync data
app.post('/api/fast-accounting/sync', (req, res) => {
  const { event_id, event_type } = req.body ?? {};
  
  if (fastDowntime) {
    console.warn(`[FAST Mock API] Rejected sync request for event ${event_id} due to simulated downtime (FAST is offline).`);
    return res.status(503).send('Service Unavailable (Simulated Downtime)');
  }
  
  // Random failure simulation (30% probability) if FAST is online, to show network resilience
  if (Math.random() < 0.3) {
    console.warn(`[FAST Mock API] Rejected sync request for event ${event_id} due to random network failure (30% probability).`);
    return res.status(503).send('Service Unavailable (Random Network Failure)');
  }
  
  console.log(`[FAST Mock API] Successfully synced event ${event_id} of type ${event_type}.`);
  res.status(200).send('OK');
});

// Create item (Transactional Outbox Pattern)
app.post('/api/items', async (req, res) => {
  const { sku, name, quantity, unit_price } = req.body ?? {};
  if (!sku || !name) {
    return res.status(400).json({ error: 'sku and name are required' });
  }
  
  // Connect a dedicated client from the pool to execute a transaction
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    // 1) Insert item
    const { rows } = await client.query(
      `INSERT INTO items (sku, name, quantity, unit_price)
       VALUES ($1, $2, $3, $4) RETURNING *`,
      [sku, name, Number(quantity) || 0, Number(unit_price) || 0],
    );
    const createdItem = rows[0];

    // 2) Write event to outbox table in the SAME transaction
    const event_id = crypto.randomUUID();
    await client.query(
      `INSERT INTO outbox_events (event_id, event_type, payload, status)
       VALUES ($1, $2, $3, $4)`,
      [event_id, 'inventory.item.created', JSON.stringify(createdItem), 'PENDING']
    );

    // Commit both operations atomically
    await client.query('COMMIT');
    
    // Publish to local Event Bus
    eventBus.publish('inventory.item.created', createdItem);
    
    res.status(201).json(createdItem);
  } catch (err: any) {
    await client.query('ROLLBACK');
    if (err?.code === '23505') {
      return res.status(409).json({ error: `SKU "${sku}" already exists` });
    }
    res.status(500).json({ error: String(err) });
  } finally {
    client.release();
  }
});

// Delete item (Transactional Outbox Pattern)
app.delete('/api/items/:id', async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    // 1) Delete item
    const { rows, rowCount } = await client.query('DELETE FROM items WHERE id = $1 RETURNING *', [req.params.id]);
    if (!rowCount) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'not found' });
    }
    const deletedItem = rows[0];

    // 2) Write event to outbox in the SAME transaction
    const event_id = crypto.randomUUID();
    await client.query(
      `INSERT INTO outbox_events (event_id, event_type, payload, status)
       VALUES ($1, $2, $3, $4)`,
      [event_id, 'inventory.item.deleted', JSON.stringify(deletedItem), 'PENDING']
    );

    await client.query('COMMIT');
    
    // Publish to local Event Bus
    eventBus.publish('inventory.item.deleted', deletedItem);
    
    res.status(204).end();
  } catch (err) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: String(err) });
  } finally {
    client.release();
  }
});

// Update item (Transactional Outbox Pattern)
app.put('/api/items/:id', async (req, res) => {
  const { id } = req.params;
  const { sku, name, quantity, unit_price } = req.body ?? {};
  if (!sku || !name) {
    return res.status(400).json({ error: 'sku and name are required' });
  }
  
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    // 1) Update item
    const { rows, rowCount } = await client.query(
      `UPDATE items
       SET sku = $1, name = $2, quantity = $3, unit_price = $4
       WHERE id = $5 RETURNING *`,
      [sku, name, Number(quantity) || 0, Number(unit_price) || 0, id],
    );
    if (!rowCount) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'not found' });
    }
    const updatedItem = rows[0];

    // 2) Write event to outbox in the SAME transaction
    const event_id = crypto.randomUUID();
    await client.query(
      `INSERT INTO outbox_events (event_id, event_type, payload, status)
       VALUES ($1, $2, $3, $4)`,
      [event_id, 'inventory.item.updated', JSON.stringify(updatedItem), 'PENDING']
    );

    await client.query('COMMIT');
    
    // Publish to local Event Bus
    eventBus.publish('inventory.item.updated', updatedItem);
    
    res.json(updatedItem);
  } catch (err: any) {
    await client.query('ROLLBACK');
    if (err?.code === '23505') {
      return res.status(409).json({ error: `SKU "${sku}" already exists` });
    }
    res.status(500).json({ error: String(err) });
  } finally {
    client.release();
  }
});

const port = Number(process.env.PORT) || 8080;
app.listen(port, () => {
  console.log(`ERP sample running at http://localhost:${port}`);
  
  // Start background outbox worker
  const outboxWorker = new OutboxWorker(port);
  outboxWorker.start();
});
