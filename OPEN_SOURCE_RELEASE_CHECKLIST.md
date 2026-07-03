# Open-source Release Checklist

Use this checklist before making PG360 public.

## Licensing and provenance

- [ ] Confirm every tracked file is original PG360 material or otherwise compatible with the chosen repository license.
- [ ] Review any material inspired by or adapted from `eDB360` / `SQLdb360`.
- [ ] If any GPL-licensed code, copied text, CSS, templates, or other protected material remains from `eDB360` / `SQLdb360`, either rewrite/remove it or change PG360 to a GPL-compatible license before publication.
- [ ] Keep the thank-you / provenance note in [ACKNOWLEDGMENTS.md](ACKNOWLEDGMENTS.md).

## Repository hygiene

- [ ] Verify `README.md` reflects the public project name, scope, and usage.
- [ ] Verify `LICENSE`, `DISCLAIMER.md`, `AUTHORS.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md` are present and accurate.
- [ ] Decide whether any untracked local files such as ad hoc demos or scratch copies should be published or ignored.
- [ ] Remove accidental workstation artifacts before pushing.

## Sample artifact review

- [ ] Re-check tracked sample HTML reports for hostnames, usernames, paths, identifiers, and sensitive operational details.
- [ ] Confirm share-safe expectations are documented clearly.
- [ ] Confirm demo SQL is labeled as non-production-only where appropriate.

## Release packaging

- [ ] Ensure the canonical runtime script is `pg360.sql`.
- [ ] Ensure the release snapshot under `versions/` matches the intended public baseline.
- [ ] Update `CHANGELOG.md` for the release being published.
- [ ] Tag the release and attach the sample artifact only if it is safe to share.

## GitHub setup

- [ ] Add a repository description and topics.
- [ ] Enable Issues and Discussions if you want community feedback in GitHub.
- [ ] Enable private vulnerability reporting if you want a built-in security channel.
- [ ] Create the first public release from a tagged version, not from an unreviewed working tree.
