/* Topics: Security Roles Privileges, Schema Object Inventory,
   Function Trigger Profile, Extension Fdw Runtime Audit */

SELECT 'Security Roles Privileges' AS report_section;

SELECT
    rolname,
    rolsuper,
    rolcreaterole,
    rolcreatedb,
    rolcanlogin,
    rolreplication,
    rolbypassrls
FROM pg_roles
ORDER BY rolsuper DESC, rolname;

SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM information_schema.role_table_grants
WHERE table_schema !~ '^pg_'
  AND table_schema <> 'information_schema'
ORDER BY grantee, table_schema, table_name, privilege_type
LIMIT 2000;

SELECT 'Schema Object Inventory' AS report_section;

SELECT
    n.nspname AS schema_name,
    c.relkind,
    count(*)::bigint AS object_count
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
GROUP BY n.nspname, c.relkind
ORDER BY n.nspname, c.relkind;

SELECT 'Function Trigger Profile' AS report_section;

SELECT
    n.nspname AS schema_name,
    count(*) FILTER (WHERE p.prokind = 'f')::bigint AS function_count,
    count(*) FILTER (WHERE p.prokind = 'p')::bigint AS procedure_count
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
GROUP BY n.nspname
ORDER BY n.nspname;

SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    t.tgname AS trigger_name,
    t.tgenabled,
    pg_get_triggerdef(t.oid, true) AS trigger_def
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT t.tgisinternal
  AND n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY n.nspname, c.relname, t.tgname
LIMIT 1000;

SELECT 'Extension Fdw Runtime Audit' AS report_section;

SELECT
    extname,
    extversion,
    n.nspname AS extension_schema
FROM pg_extension e
JOIN pg_namespace n ON n.oid = e.extnamespace
ORDER BY extname;

SELECT
    fdwname,
    fdwhandler::regproc AS handler,
    fdwvalidator::regproc AS validator,
    fdwacl
FROM pg_foreign_data_wrapper
ORDER BY fdwname;

SELECT
    s.srvname,
    f.fdwname,
    s.srvtype,
    s.srvversion,
    s.srvoptions
FROM pg_foreign_server s
JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
ORDER BY s.srvname;
