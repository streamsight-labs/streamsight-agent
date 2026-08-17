### What does this PR do?

<!-- One or two sentences. -->

### Motivation

<!-- What was wrong, or what became possible. Link an issue if there is one. -->

### How did you validate this change?

<!--
The important one. Much of this repo's correctness is only observable against a
real broker, so "go test passes" is not on its own an answer for a collector or
export change. Say what you actually ran: unit tests, `go test -race`, the local
SASL+ACL cluster in test/, a specific broker version, a sample batch you
inspected. If you changed what goes on the wire, paste the before/after of the
affected part of the batch.
-->

### Does this change the ACL requirement?

<!--
The agent requires exactly DESCRIBE on CLUSTER, TOPIC and GROUP, never reads
record data, and never writes to the cluster (see SECURITY.md). Answer "no", or
stop here and open an issue -- a fourth grant is a product decision, not a
review comment.
-->

- [ ] No new Kafka permission is required.
- [ ] `make check` and `go test -race ./...` pass locally.
- [ ] The wire contract (`internal/metrics/types.go`) is unchanged, or the
      change is described above.

### Additional notes

<!-- Anything a reviewer would otherwise have to ask. Delete if empty. -->
