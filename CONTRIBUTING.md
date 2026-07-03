# Contributing

Thanks for helping improve PG360.

## What to optimize for

- Keep `pg360.sql` read-only.
- Preserve the single-script execution model.
- Degrade gracefully when optional views or extensions are unavailable.
- Keep wording honest about what is fact, derived signal, or heuristic.
- Maintain compatibility across PostgreSQL `15` through `18`, and keep PostgreSQL `19` prerelease compatibility in view unless a change is explicitly version-gated.

## Before opening a change

- Search existing issues or release notes first.
- If the change affects report output, include a short explanation of the operator value.
- If the change is version-specific or managed-service-specific, say so clearly.

## Pull request expectations

- Describe the problem and the behavioral change.
- Call out any PostgreSQL version assumptions.
- Mention validation performed.
- Update docs when user-facing behavior changes.
- Keep sample output sanitized before committing it.

## Safety rules for SQL changes

- No DDL or DML against user data.
- No hidden writes, temp tables, or side effects.
- No dependency on external CSS, JavaScript, or helper scripts for the main runtime path.
- Guard optional telemetry with existence and privilege checks.
- Prefer clear blocked-state output over runtime failure.

## Reporting bugs

- Include PostgreSQL version, environment type, and a minimal reproduction when possible.
- Remove hostnames, usernames, query text, customer names, and other sensitive data before posting.
- If the issue could expose credentials, private SQL text, or unintended write behavior, follow [SECURITY.md](SECURITY.md) instead of opening a detailed public issue.
