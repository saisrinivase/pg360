# Security Policy

## Supported scope

Security fixes are expected to focus on the current `pg360.sql` runtime and the latest documented release snapshot.

## What to report

Please report issues such as:

- unintended writes or side effects from the report
- exposure of secrets, credentials, or sensitive SQL text in generated output
- privilege escalation requirements that are broader than documented
- command execution, file access, or unsafe server-side capabilities
- redaction failures in share-safe or sanitized output modes

## How to report

- If GitHub private vulnerability reporting is enabled for the repository, use that channel.
- Otherwise, do not post full exploit details in a public issue when the report could expose real systems.
- Open a minimal public issue asking for a private contact path, or use the maintainer contact method published on the repository profile.

## What to include

- affected PG360 version or commit
- PostgreSQL version and environment type
- impact summary
- safe reproduction notes or sanitized evidence
- whether the issue is already being exploited, if known

## Handling guidance

- Please keep details private until the maintainer confirms a fix or mitigation path.
- After a fix is available, a public summary can be added to the changelog or release notes.
