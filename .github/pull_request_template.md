## What and why

<!-- What changes, and what forced the change. If you discovered something
while implementing, say what — that is the part a reader in six months needs. -->

## Checklist

- [ ] `make check` passes
- [ ] New behaviour has a test that fails without this change
- [ ] Backend behaviour changes are in `dagstoretest`, not special-cased in the Manager
- [ ] A design change has an ADR (new file in `docs/adr/`, amending rather than editing)
- [ ] A performance claim has a `benchstat` comparison in the description
