# Contributing

Small repo, one maintainer, short rules. Behaviour in issues and reviews is covered by
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md), which is the Contributor Covenant and holds no
surprises.

## Before you push

```bash
make check           # gofmt -l, go vet, go test
go test -race ./...  # required, see below
golangci-lint run    # config is .golangci.yml
```

`-race` is not optional. The collector fans twelve goroutines out per cycle and joins them
through channel closes, and the exporters share counters with a background worker — a data
race here is a wrong number in a customer's dashboard, which is worse than a crash because
nobody notices. CI runs the same gates; it is cheaper to find out locally.

## Commits

**One line. Conventional commit. Nothing else.**

```
fix(collector): keep partial results when one broker shards out
```

- Prefixes in use: `feat`, `fix`, `docs`, `refactor`, `chore`, `test`, `ci`,
  `deps`. An optional scope in parentheses.
- **No body.** However large the diff, compress it into the one line. A
  40-file change still gets one line. Explanation belongs in the PR
  description, or better, in a comment next to the code it explains.
- **No attribution trailers.** No `Co-Authored-By:` for tooling, no
  `Generated with …`, no session links. `git log` is the change history, not a
  credits roll.

## Code

- **Comments explain *why*, never *what*.** The code already says what it
  does. Every non-obvious decision carries the reason it was made, because the
  next person to read it will otherwise "simplify" it back into the bug it was
  written to avoid. `internal/collector/errors.go` and
  `internal/config/config.go` are the calibration.
- **Nullable numerics are `*int64` / `*int32`.** `null` means "not known this
  cycle"; it never means zero. A failed offset lookup and an empty partition
  must stay distinguishable on the wire.
- **Deterministic output.** Sort everything — kadm's `Sorted()` variants exist
  for this. Never iterate a Go map into a batch; map order is randomised and
  will silently flap a field between cycles.
- **Section names and error kinds are wire contract**, so they are constants,
  and changing one is a schema change.
- **Config is parsed and validated in `config.Load`** via the `p.integer` /
  `p.boolean` / `p.duration` helpers, reporting *all* failures at once rather
  than the first. A new field also needs its default in the const block and a
  line in `Redacted()` and `Warnings()`.
- **stdlib and `log/slog` only.** No framework, no `golang.org/x/sync`. New
  module dependencies are a discussion, not a PR — a customer's security team
  reads this dependency list.
- **The agent computes nothing.** It ships raw inventory and offset snapshots;
  all lag, rates, and aggregates are the backend's job. A PR that adds a
  derived metric is almost always in the wrong repository.

## The ACL invariant

The agent requires exactly five read-only grants: `DESCRIBE` on `CLUSTER`, on
`TOPIC` and on `GROUP`, plus `DESCRIBE_CONFIGS` on `TOPIC` and on `CLUSTER`. It
never reads record data and never writes to the cluster. That is not a
description of the current implementation — it is the product, and it is what
[SECURITY.md](SECURITY.md) promises to the people who decide whether this binary
may run against their production Kafka. `DESCRIBE_CONFIGS` is read-only and is
not `ALTER_CONFIGS`; why it is in the set is in [SECURITY.md](SECURITY.md).

**A change that requires a sixth ACL is a product decision. Open an issue
first.** It will not be merged as part of a feature PR, however good the
feature is. Two candidate collectors are already parked on exactly this
question.

If you are unsure whether a new Admin API call stays inside those grants,
`make test-local-up` brings up a KRaft cluster with SASL/SCRAM and
`StandardAuthorizer` enforcing the three `DESCRIBE`s and nothing else — see
[docs/TESTING.md](docs/TESTING.md#3-the-permission-model-sasl--acls). Revoking
one grant and watching a single section turn `unauthorized` while the others
stay `ok` is the fastest way to prove an attribution change works.
`make test-local-urp` does the same for `reassignments`, which needs an
under-replicated partition before it issues a request at all.

## Pull requests

Fill in the template. The line that matters most is *how you validated the
change*: much of this repo's correctness is only observable against a live
broker, and "tests pass" does not cover a claim about what Kafka returns.

Keep PRs to one concern. A refactor and a fix in one diff cost more to review
than both separately.
