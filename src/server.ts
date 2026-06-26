import express from 'express';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { pool } from './db';
import { eventBus } from './eventBus';
import { OutboxWorker } from './outboxWorker';
import { initProjector } from './projector';

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

// 1) List lifecycle orders (Read Model)
app.get('/api/lifecycle/orders', async (_req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM order_lifecycle_view ORDER BY updated_at DESC');
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

// 2) Create new random order (Write Model & publishes order.created)
app.post('/api/orders', async (req, res) => {
  const { customer_name, items: reqItems } = req.body ?? {};
  
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    // Auto-generate order number: SO-2026-XXXX (4 random digits)
    const order_number = `SO-2026-${Math.floor(Math.random() * 9000) + 1000}`;
    const name = customer_name || `Khách hàng ${crypto.randomUUID().slice(0, 5)}`;
    
    let orderLines = reqItems || [];
    if (orderLines.length === 0) {
      // Fetch 1-3 active items from items table to create realistic lines
      const itemsRes = await client.query('SELECT sku, unit_price FROM items LIMIT 3');
      if (itemsRes.rows.length === 0) {
        throw new Error('No items in warehouse to create an order. Please create items first.');
      }
      
      const count = Math.floor(Math.random() * itemsRes.rows.length) + 1;
      for (let i = 0; i < count; i++) {
        orderLines.push({
          sku: itemsRes.rows[i].sku,
          quantity: Math.floor(Math.random() * 5) + 1,
          unit_price: Number(itemsRes.rows[i].unit_price)
        });
      }
    }
    
    const total_amount = orderLines.reduce((sum: number, it: any) => sum + (it.quantity * it.unit_price), 0);
    
    const oRes = await client.query(
      `INSERT INTO orders (order_number, customer_name, total_amount, status)
       VALUES ($1, $2, $3, 'DRAFT') RETURNING *`,
      [order_number, name, total_amount]
    );
    const order = oRes.rows[0];
    
    for (const line of orderLines) {
      await client.query(
        `INSERT INTO order_items (order_id, sku, quantity, unit_price)
         VALUES ($1, $2, $3, $4)`,
        [order.id, line.sku, line.quantity, line.unit_price]
      );
    }
    
    await client.query('COMMIT');
    
    eventBus.publish('order.created', { order, items: orderLines });
    
    res.status(201).json(order);
  } catch (err: any) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: err.message || String(err) });
  } finally {
    client.release();
  }
});

// 3) Advance order lifecycle (updates write model and publishes domain events)
app.post('/api/orders/:order_number/advance', async (req, res) => {
  const { order_number } = req.params;
  
  const client = await pool.connect();
  try {
    const viewRes = await pool.query('SELECT current_stage, stage_status FROM order_lifecycle_view WHERE order_number = $1', [order_number]);
    if (viewRes.rows.length === 0) {
      return res.status(404).json({ error: 'Order not found in lifecycle view' });
    }
    
    const { current_stage, stage_status } = viewRes.rows[0];
    
    if (stage_status === 'stuck') {
      return res.status(400).json({ error: 'Đơn hàng đang bị kẹt sự cố. Hãy giải quyết sự cố trước.' });
    }
    if (stage_status === 'returned') {
      return res.status(400).json({ error: 'Đơn hàng đã bị hoàn trả, không thể tiến hành.' });
    }
    if (current_stage === 9 && stage_status === 'completed') {
      return res.status(400).json({ error: 'Đơn hàng đã ở chặng Hoàn thành (Closed).' });
    }
    
    await client.query('BEGIN');
    
    let nextStatus = '';
    let eventType = '';
    let payload: any = { order_number };

    if (current_stage === 2) {
      nextStatus = 'PROCURING';
      eventType = 'sourcing.completed';
    } else if (current_stage === 3) {
      nextStatus = 'WMS_ALLOCATED';
      eventType = 'wms.allocated';
    } else if (current_stage === 4) {
      nextStatus = 'WMS_PICKED_PACKED';
      eventType = 'wms.packed';
    } else if (current_stage === 5) {
      nextStatus = 'SHIPPING';
      eventType = 'logistics.shipped';
      payload.tracking_number = `VTP-${crypto.randomUUID().slice(0, 8).toUpperCase()}`;
    } else if (current_stage === 6) {
      nextStatus = 'DELIVERED';
      eventType = 'logistics.delivered';
    } else if (current_stage === 7) {
      nextStatus = 'RECONCILED';
      eventType = 'payment.reconciled';
      payload.payment_amount = 0;
      payload.transaction_id = `BANK-${crypto.randomUUID().slice(0, 12).toUpperCase()}`;
      
      const oRes = await client.query('SELECT total_amount FROM orders WHERE order_number = $1', [order_number]);
      if (oRes.rows.length > 0) {
        payload.payment_amount = Number(oRes.rows[0].total_amount);
      }
    } else if (current_stage === 8) {
      nextStatus = 'CLOSED';
      eventType = 'order.closed';
    } else if (current_stage === 1) {
      nextStatus = 'APPROVED';
      eventType = 'order.approved';
    }

    await client.query('UPDATE orders SET status = $1 WHERE order_number = $2', [nextStatus, order_number]);
    
    // TÍCH HỢP PHẦN 1: Outbox Event để đồng bộ hóa đơn sang FAST khi chuyển sang Shipping
    if (eventType === 'logistics.shipped') {
      const orderRes = await client.query('SELECT * FROM orders WHERE order_number = $1', [order_number]);
      const oData = orderRes.rows[0];
      const event_id = crypto.randomUUID();
      await client.query(
        `INSERT INTO outbox_events (event_id, event_type, payload, status)
         VALUES ($1, $2, $3, $4)`,
        [
          event_id, 
          'fast.invoice.created', 
          JSON.stringify({ 
            order_number: oData.order_number, 
            customer_name: oData.customer_name, 
            total_amount: Number(oData.total_amount),
            tracking_number: payload.tracking_number
          }), 
          'PENDING'
        ]
      );
    }
    
    await client.query('COMMIT');
    
    eventBus.publish(eventType, payload);
    
    res.json({ success: true, order_number, nextStatus, current_stage: current_stage + 1 });
  } catch (err) {
    await client.query('ROLLBACK');
    res.status(500).json({ error: String(err) });
  } finally {
    client.release();
  }
});

// 4) Toggle order stuck status
app.post('/api/orders/:order_number/stuck', async (req, res) => {
  const { order_number } = req.params;
  const { reason } = req.body ?? {};
  
  try {
    const { rows } = await pool.query('SELECT stage_status FROM order_lifecycle_view WHERE order_number = $1', [order_number]);
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }
    
    const isStuckNow = rows[0].stage_status === 'stuck';
    const nextStuck = !isStuckNow;
    
    const nextStatus = nextStuck ? 'STUCK' : 'APPROVED';
    await pool.query('UPDATE orders SET status = $1 WHERE order_number = $2', [nextStatus, order_number]);
    
    eventBus.publish('order.stuck_toggled', {
      order_number,
      is_stuck: nextStuck,
      reason: nextStuck ? (reason || 'Sự cố phát sinh ngoài ý muốn (Logistics delay)') : null
    });
    
    res.json({ success: true, is_stuck: nextStuck });
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

// 5) Return order
app.post('/api/orders/:order_number/return', async (req, res) => {
  const { order_number } = req.params;
  const { reason } = req.body ?? {};
  
  try {
    await pool.query("UPDATE orders SET status = 'RETURNED' WHERE order_number = $1", [order_number]);
    
    eventBus.publish('order.returned', {
      order_number,
      reason: reason || 'Khách hàng yêu cầu đổi trả (D-053)'
    });
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

const port = Number(process.env.PORT) || 8080;
app.listen(port, () => {
  console.log(`ERP sample running at http://localhost:${port}`);
  
  // Start background outbox worker
  const outboxWorker = new OutboxWorker(port);
  outboxWorker.start();

  // Start order lifecycle projector
  initProjector();
});
