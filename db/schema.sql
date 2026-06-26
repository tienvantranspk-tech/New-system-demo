-- ERP sample schema: a tiny inventory / items table
CREATE TABLE IF NOT EXISTS items (
  id          SERIAL PRIMARY KEY,
  sku         TEXT NOT NULL UNIQUE,
  name        TEXT NOT NULL,
  quantity    INTEGER NOT NULL DEFAULT 0,
  unit_price  NUMERIC(12,2) NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed a few rows (idempotent)
INSERT INTO items (sku, name, quantity, unit_price) VALUES
  ('SKU-001', 'Thung carton 60x40', 120, 15000),
  ('SKU-002', 'Bang keo trong 5cm', 480, 8000),
  ('SKU-003', 'Pallet go 1m2', 35, 250000)
ON CONFLICT (sku) DO NOTHING;

-- Audit events log for Pub/Sub tracking
CREATE TABLE IF NOT EXISTS audit_events (
  id           SERIAL PRIMARY KEY,
  event_id     TEXT NOT NULL UNIQUE,
  event_type   TEXT NOT NULL,
  payload      JSONB NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional Outbox Events log for FAST integration sync
CREATE TABLE IF NOT EXISTS outbox_events (
  id           SERIAL PRIMARY KEY,
  event_id     TEXT NOT NULL UNIQUE,
  event_type   TEXT NOT NULL,
  payload      JSONB NOT NULL,
  status       TEXT NOT NULL DEFAULT 'PENDING',
  attempts     INTEGER NOT NULL DEFAULT 0,
  last_attempt TIMESTAMPTZ,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Orders Write Model Table
CREATE TABLE IF NOT EXISTS orders (
  id              SERIAL PRIMARY KEY,
  order_number    TEXT NOT NULL UNIQUE,
  customer_name   TEXT NOT NULL,
  total_amount    NUMERIC(12,2) NOT NULL DEFAULT 0,
  status          TEXT NOT NULL DEFAULT 'DRAFT',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Order Items Table
CREATE TABLE IF NOT EXISTS order_items (
  id              SERIAL PRIMARY KEY,
  order_id        INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  sku             TEXT NOT NULL,
  quantity        INTEGER NOT NULL,
  unit_price      NUMERIC(12,2) NOT NULL
);

-- Order Lifecycle View Read Model Table
CREATE TABLE IF NOT EXISTS order_lifecycle_view (
  order_number    TEXT PRIMARY KEY,
  customer_name   TEXT NOT NULL,
  total_amount    NUMERIC(12,2) NOT NULL,
  current_stage   INTEGER NOT NULL DEFAULT 1,
  stage_status    TEXT NOT NULL DEFAULT 'active', -- 'active', 'completed', 'stuck', 'returned'
  history         JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
