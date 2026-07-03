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

CREATE TABLE IF NOT EXISTS pg360_demo.deploy_summary (
  deploy_id integer PRIMARY KEY,
  event_count integer NOT NULL DEFAULT 0,
  metric_sum bigint NOT NULL DEFAULT 0,
  last_refresh timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS pg360_demo.tenant_orders (
  order_id integer PRIMARY KEY,
  tenant_id integer NOT NULL,
  status text NOT NULL,
  amount numeric(12,2) NOT NULL,
  note text
);

CREATE TABLE IF NOT EXISTS pg360_demo.trigger_heavy_events (
  event_id integer PRIMARY KEY,
  tenant_id integer NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  revision integer NOT NULL DEFAULT 0,
  audit_note text
);

CREATE TABLE IF NOT EXISTS pg360_demo.trigger_heavy_audit (
  audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id integer,
  action text NOT NULL,
  changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  note text
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

INSERT INTO pg360_demo.tenant_orders (order_id, tenant_id, status, amount, note)
VALUES
  (1, 10, 'open',     1250.00, 'pg360 validation tenant order 1'),
  (2, 10, 'past_due',  880.00, 'pg360 validation tenant order 2'),
  (3, 11, 'open',      415.00, 'pg360 validation tenant order 3'),
  (4, 12, 'closed',    990.00, 'pg360 validation tenant order 4')
ON CONFLICT (order_id) DO UPDATE
SET tenant_id = EXCLUDED.tenant_id,
    status = EXCLUDED.status,
    amount = EXCLUDED.amount,
    note = EXCLUDED.note;

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

CREATE OR REPLACE FUNCTION pg360_demo.volatile_public_event_count(p_deploy_id integer)
RETURNS bigint
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
AS $$
DECLARE
  v bigint;
BEGIN
  PERFORM pg_sleep(0.025);

  SELECT
    COALESCE((SELECT count(*)::bigint
              FROM pg360_demo.deploy_events
              WHERE deploy_id = p_deploy_id), 0) +
    COALESCE((SELECT sum(metric_value)::bigint
              FROM pg360_demo.deploy_metrics
              WHERE deploy_id = p_deploy_id), 0)
  INTO v;

  RETURN v;
END
$$;

CREATE OR REPLACE PROCEDURE pg360_demo.refresh_public_summary(p_deploy_id integer)
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO pg360_demo.deploy_summary (deploy_id, event_count, metric_sum, last_refresh)
  VALUES (
    p_deploy_id,
    (SELECT count(*) FROM pg360_demo.deploy_events WHERE deploy_id = p_deploy_id),
    COALESCE((SELECT sum(metric_value)::bigint FROM pg360_demo.deploy_metrics WHERE deploy_id = p_deploy_id), 0),
    clock_timestamp()
  )
  ON CONFLICT (deploy_id) DO UPDATE
  SET event_count = EXCLUDED.event_count,
      metric_sum = EXCLUDED.metric_sum,
      last_refresh = EXCLUDED.last_refresh;
END
$$;

CREATE OR REPLACE VIEW pg360_demo.tenant_order_exposure_v AS
SELECT
  tenant_id,
  order_id,
  status,
  amount,
  note
FROM pg360_demo.tenant_orders
WHERE status IN ('open', 'past_due');

CREATE OR REPLACE FUNCTION pg360_demo.tg_prepare_trigger_heavy_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  NEW.payload := COALESCE(NEW.payload, '{}'::jsonb);
  NEW.audit_note := COALESCE(NEW.audit_note, 'pg360 validation trigger row');
  RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION pg360_demo.tg_bump_trigger_heavy_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.revision := COALESCE(OLD.revision, 0) + 1;
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION pg360_demo.tg_audit_trigger_heavy_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO pg360_demo.trigger_heavy_audit (event_id, action, note)
  VALUES (
    COALESCE(NEW.event_id, OLD.event_id),
    TG_OP,
    'pg360 validation trigger audit'
  );

  RETURN COALESCE(NEW, OLD);
END
$$;

DROP TRIGGER IF EXISTS trg_heavy_events_bi_prepare ON pg360_demo.trigger_heavy_events;
DROP TRIGGER IF EXISTS trg_heavy_events_bu_revision ON pg360_demo.trigger_heavy_events;
DROP TRIGGER IF EXISTS trg_heavy_events_ai_audit ON pg360_demo.trigger_heavy_events;
DROP TRIGGER IF EXISTS trg_heavy_events_au_audit ON pg360_demo.trigger_heavy_events;
DROP TRIGGER IF EXISTS trg_heavy_events_bd_audit ON pg360_demo.trigger_heavy_events;

CREATE TRIGGER trg_heavy_events_bi_prepare
BEFORE INSERT ON pg360_demo.trigger_heavy_events
FOR EACH ROW
EXECUTE FUNCTION pg360_demo.tg_prepare_trigger_heavy_event();

CREATE TRIGGER trg_heavy_events_bu_revision
BEFORE UPDATE ON pg360_demo.trigger_heavy_events
FOR EACH ROW
EXECUTE FUNCTION pg360_demo.tg_bump_trigger_heavy_revision();

CREATE TRIGGER trg_heavy_events_ai_audit
AFTER INSERT ON pg360_demo.trigger_heavy_events
FOR EACH ROW
EXECUTE FUNCTION pg360_demo.tg_audit_trigger_heavy_event();

CREATE TRIGGER trg_heavy_events_au_audit
AFTER UPDATE ON pg360_demo.trigger_heavy_events
FOR EACH ROW
EXECUTE FUNCTION pg360_demo.tg_audit_trigger_heavy_event();

CREATE TRIGGER trg_heavy_events_bd_audit
BEFORE DELETE ON pg360_demo.trigger_heavy_events
FOR EACH ROW
EXECUTE FUNCTION pg360_demo.tg_audit_trigger_heavy_event();

REVOKE ALL ON FUNCTION pg360_demo.sum_invoice_items(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg360_demo.customer_order_count(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg360_demo.tg_prepare_trigger_heavy_event() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg360_demo.tg_bump_trigger_heavy_revision() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg360_demo.tg_audit_trigger_heavy_event() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg360_demo.volatile_public_event_count(integer) FROM PUBLIC;
REVOKE ALL ON PROCEDURE pg360_demo.refresh_public_summary(integer) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pg360_demo.volatile_public_event_count(integer) TO PUBLIC;
GRANT EXECUTE ON PROCEDURE pg360_demo.refresh_public_summary(integer) TO PUBLIC;
GRANT SELECT ON pg360_demo.tenant_order_exposure_v TO PUBLIC;

SELECT pg360_demo.sum_invoice_items(1) FROM generate_series(1,6);
SELECT pg360_demo.customer_order_count(42) FROM generate_series(1,4);
SELECT pg360_demo.volatile_public_event_count(((gs - 1) % 12) + 1)
FROM generate_series(1,50) AS gs;

CALL pg360_demo.refresh_public_summary(5);
CALL pg360_demo.refresh_public_summary(12);
CALL pg360_demo.refresh_public_summary(18);

INSERT INTO pg360_demo.trigger_heavy_events (event_id, tenant_id, payload, audit_note)
VALUES
  (1, 10, '{"kind":"order","status":"new"}'::jsonb, 'pg360 validation trigger row 1'),
  (2, 11, '{"kind":"order","status":"queued"}'::jsonb, 'pg360 validation trigger row 2'),
  (3, 12, '{"kind":"order","status":"retry"}'::jsonb, 'pg360 validation trigger row 3')
ON CONFLICT (event_id) DO UPDATE
SET tenant_id = EXCLUDED.tenant_id,
    payload = EXCLUDED.payload,
    audit_note = EXCLUDED.audit_note;

UPDATE pg360_demo.trigger_heavy_events
SET payload = payload || jsonb_build_object('touched_by', 'pg360_validation_seed'),
    audit_note = 'pg360 validation trigger update'
WHERE event_id IN (1, 2);

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
