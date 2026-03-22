\set ON_ERROR_STOP on
SET application_name = 'pg360_validation_seed';
SET track_functions = 'pl';
SET plan_cache_mode = auto;

CREATE SCHEMA IF NOT EXISTS pg360_demo;

CREATE TABLE IF NOT EXISTS pg360_demo.lock_probe (
  id integer PRIMARY KEY,
  note text
);

CREATE TABLE IF NOT EXISTS pg360_demo.deploy_parent (
  id integer PRIMARY KEY,
  note text
);

CREATE TABLE IF NOT EXISTS pg360_demo.deploy_events (
  event_id integer PRIMARY KEY,
  deploy_id integer NOT NULL REFERENCES pg360_demo.deploy_parent(id),
  note text
);

CREATE TABLE IF NOT EXISTS pg360_demo.deploy_metrics (
  metric_id integer PRIMARY KEY,
  deploy_id integer NOT NULL REFERENCES pg360_demo.deploy_parent(id),
  metric_value integer
);

CREATE SEQUENCE IF NOT EXISTS pg360_demo.near_exhaustion_id_seq
  AS integer
  START WITH 1
  INCREMENT BY 1
  MINVALUE 1
  MAXVALUE 20
  CACHE 1
  NO CYCLE;

CREATE TABLE IF NOT EXISTS pg360_demo.near_exhaustion_probe (
  id integer PRIMARY KEY DEFAULT nextval('pg360_demo.near_exhaustion_id_seq'),
  note text
);

ALTER SEQUENCE pg360_demo.near_exhaustion_id_seq
  AS integer
  MINVALUE 1
  MAXVALUE 20
  INCREMENT BY 1
  CACHE 1
  NO CYCLE;

ALTER SEQUENCE pg360_demo.near_exhaustion_id_seq
  OWNED BY pg360_demo.near_exhaustion_probe.id;

INSERT INTO pg360_demo.lock_probe (id, note)
VALUES (1, 'pg360 validation lock target')
ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note;

INSERT INTO pg360_demo.deploy_parent (id, note)
SELECT gs, 'pg360 validation deploy ' || gs
FROM generate_series(1, 40) AS gs
ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note;

INSERT INTO pg360_demo.deploy_events (event_id, deploy_id, note)
SELECT gs, ((gs - 1) % 40) + 1, 'pg360 validation deploy event ' || gs
FROM generate_series(1, 4000) AS gs
ON CONFLICT (event_id) DO UPDATE SET deploy_id = EXCLUDED.deploy_id, note = EXCLUDED.note;

INSERT INTO pg360_demo.deploy_metrics (metric_id, deploy_id, metric_value)
SELECT gs, ((gs - 1) % 40) + 1, (gs % 100)
FROM generate_series(1, 4000) AS gs
ON CONFLICT (metric_id) DO UPDATE SET deploy_id = EXCLUDED.deploy_id, metric_value = EXCLUDED.metric_value;

SELECT count(*) FROM pg360_demo.deploy_events WHERE deploy_id = 5;
SELECT count(*) FROM pg360_demo.deploy_events WHERE deploy_id = 12;
SELECT count(*) FROM pg360_demo.deploy_metrics WHERE deploy_id = 5;
SELECT count(*) FROM pg360_demo.deploy_metrics WHERE deploy_id = 12;

INSERT INTO pg360_demo.near_exhaustion_probe (id, note)
SELECT gs, 'pg360 validation near-exhaustion row ' || gs
FROM generate_series(1, 18) AS gs
ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note;

SELECT setval('pg360_demo.near_exhaustion_id_seq', 18, true);

CREATE OR REPLACE FUNCTION pg360_demo.sum_invoice_items(p_qty integer)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  v bigint;
BEGIN
  SELECT count(*) INTO v
  FROM miglab.invoice_items
  WHERE quantity >= p_qty;
  RETURN v;
END
$$;

CREATE OR REPLACE FUNCTION pg360_demo.customer_order_count(p_customer integer)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  v bigint;
BEGIN
  SELECT count(*) INTO v
  FROM miglab.sales_orders so
  WHERE so.customer_id = p_customer;
  RETURN v;
END
$$;

SELECT pg360_demo.sum_invoice_items(1) FROM generate_series(1,6);
SELECT pg360_demo.customer_order_count(42) FROM generate_series(1,4);

PREPARE pg360_demo_item_count(integer) AS
SELECT count(*)
FROM miglab.invoice_items
WHERE quantity >= $1;

EXECUTE pg360_demo_item_count(1);
EXECUTE pg360_demo_item_count(2);
EXECUTE pg360_demo_item_count(3);
EXECUTE pg360_demo_item_count(4);
EXECUTE pg360_demo_item_count(5);
EXECUTE pg360_demo_item_count(6);

PREPARE pg360_demo_customer_orders(integer) AS
SELECT count(*)
FROM miglab.sales_orders
WHERE customer_id = $1;

EXECUTE pg360_demo_customer_orders(1);
EXECUTE pg360_demo_customer_orders(2);
EXECUTE pg360_demo_customer_orders(3);
EXECUTE pg360_demo_customer_orders(4);
