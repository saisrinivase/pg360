\set ON_ERROR_STOP on
SET application_name = 'pg360_validation_seed';
SET track_functions = 'pl';
SET plan_cache_mode = auto;

CREATE SCHEMA IF NOT EXISTS pg360_demo;

CREATE TABLE IF NOT EXISTS pg360_demo.lock_probe (
  id integer PRIMARY KEY,
  note text
);

INSERT INTO pg360_demo.lock_probe (id, note)
VALUES (1, 'pg360 validation lock target')
ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note;

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
