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
