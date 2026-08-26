package collector

import (
	"context"
	"errors"
	"math"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"kafka-metrics-agent/internal/metrics"
)

// authorizedOpsOmitted is Kafka's AUTHORIZED_OPERATIONS_OMITTED. A broker sends
// it when it did not consult the authorizer at all; it sends 0 when it did and
// the principal may do nothing. Those are opposite answers and both decode to
// an empty operation list, which is why this phase reads the raw bitfield.
const authorizedOpsOmitted int32 = math.MinInt32

var (
	errTopicNotDescribed   = errors.New("metadata response omitted the topic")
	errNotMetadataResponse = errors.New("broker answered Metadata with another response type")
)

// collectAuthorizedOps asks the brokers what this agent's own principal is
// allowed to do, so a backend can turn "section: unauthorized" into "grant
// DESCRIBE_CONFIGS on topic X to principal Y".
//
// It grants nothing and reads no records, and it needs no ACL beyond the
// DESCRIBE on CLUSTER, TOPIC and GROUP the agent already requires: KIP-430
// bitfields ride requests the agent is already permitted to make.
//
// Two sources, for two different reasons:
//
//   - Groups are free. kadm sets IncludeAuthorizedOperations on every
//     DescribeGroups (kadm@v1.18.0 groups.go:347), so the groups phase already
//     paid for the bitfield and this only surfaces it. It arrives decoded.
//   - Topics and the cluster need this phase's own request. kadm's Metadata is
//     served by kgo's RequestCachedMetadata, which documents that it never
//     returns authorized operations however the request is built (kgo@v1.21.6
//     client.go:1506-1508). kadm.WithAuthorizedOps switches that call to a
//     direct one, but the cluster phase's Metadata is also what warms the cache
//     the three List*Offsets calls read through (kadm listOffsets -> ListTopics
//     -> Metadata), so bypassing it moves the fetch rather than removing it: the
//     same one extra Metadata per cycle, minus the raw bitfield, which is the
//     one thing this phase cannot work without.
//
// run is false on the cycles between samples; the section is still built and
// returned, so "not collected" and "collected, nothing permitted" are never the
// same batch. The cadence exists because grants change on human timescales while
// the request is O(partitions) of the topics already selected.
//
// It runs after the topics and groups phases because it stamps their entries.
func (c *Collector) collectAuthorizedOps(ctx context.Context, batch *metrics.Batch, described kadm.DescribedGroups, run bool) *section {
	sec := c.newSection(sectionAuthorizedOps)
	defer sec.stop()

	if !run || batch == nil {
		sec.downgrade(metrics.SectionSkipped)
		return sec
	}

	applyGroupAuthorizedOps(batch.Groups, described)

	names := make([]string, 0, len(batch.Topics))
	for _, t := range batch.Topics {
		names = append(names, t.Name)
	}

	resp, err := c.requestAuthorizedOps(ctx, names)
	if !sec.request("Metadata", err) {
		return sec
	}

	batch.Cluster.AuthorizedOperations = authorizedOps(resp.AuthorizedOperations)
	applyTopicAuthorizedOps(sec, batch.Topics, resp)
	return sec
}

// requestAuthorizedOps issues the one Metadata request this phase owns.
func (c *Collector) requestAuthorizedOps(ctx context.Context, names []string) (*kmsg.MetadataResponse, error) {
	kresp, err := c.client.Request(ctx, authorizedOpsRequest(names))
	if err != nil {
		return nil, err
	}
	resp, ok := kresp.(*kmsg.MetadataResponse)
	if !ok {
		return nil, errNotMetadataResponse
	}
	return resp, nil
}

// authorizedOpsRequest builds the Metadata request, separately from issuing it,
// because the two flags and the empty-topics rule below are the whole contract.
func authorizedOpsRequest(names []string) *kmsg.MetadataRequest {
	req := kmsg.NewPtrMetadataRequest()
	req.IncludeTopicAuthorizedOperations = true
	// Kafka 2.8 removed the cluster bitfield from Metadata (v11) in favour of
	// DescribeCluster, and kmsg stops encoding the flag above v10, so this asks
	// the brokers that still answer and costs nothing on the ones that do not.
	req.IncludeClusterAuthorizedOperations = true

	// Non-nil and empty means "no topics"; NIL means "every topic in the
	// cluster", which would undo the agent's topic filter and its cap. Same
	// class of trap as the List*Offsets and DescribeLogDirs guards.
	req.Topics = make([]kmsg.MetadataRequestTopic, 0, len(names))
	for _, name := range names {
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = kmsg.StringPtr(name)
		req.Topics = append(req.Topics, t)
	}
	// AllowAutoTopicCreation is left false: a topic deleted mid-cycle is still
	// named here, and a read-only agent must not recreate it.
	return req
}

// applyTopicAuthorizedOps stamps each topic with what the principal may do to
// it. A topic the response did not name at all is recorded and left unknown:
// the broker echoes every requested name, erroring ones included, so a missing
// one is an anomaly rather than a deletion.
func applyTopicAuthorizedOps(sec *section, topics []metrics.TopicMetrics, resp *kmsg.MetadataResponse) {
	bits := make(map[string]int32, len(resp.Topics))
	for _, t := range resp.Topics {
		// Topics are requested by name, so the broker echoes names; a nil name
		// would be a response to an ID-only request and belongs to nothing here.
		if t.Topic == nil {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			// Worth recording even though the cluster phase saw the same topic:
			// TOPIC_AUTHORIZATION_FAILED on this request is the finding, not a
			// side effect. Kafka builds an errored topic's metadata without
			// consulting the authorizer, so its bitfield is the omitted sentinel
			// and the topic correctly stays unknown below.
			sec.recordTopic("Metadata", *t.Topic, err)
		}
		bits[*t.Topic] = t.AuthorizedOperations
	}

	for i := range topics {
		b, ok := bits[topics[i].Name]
		if !ok {
			sec.recordTopic("Metadata", topics[i].Name, errTopicNotDescribed)
			continue
		}
		topics[i].AuthorizedOperations = authorizedOps(b)
	}
}

// applyGroupAuthorizedOps stamps the bitfields the groups phase already
// received onto the emitted groups.
//
// kadm decodes before the agent sees the response and collapses the omitted
// sentinel and a genuine zero to the same nil slice, so emptiness is shippable
// as "nothing permitted" only because two facts establish that the broker
// evaluated the authorizer. First, the phase runs only when the capability
// probe found Metadata v8+, and DescribeGroups gained the field in v3: both are
// Kafka 2.3 (KIP-430), so the one gate covers both requests. Second, Kafka
// builds an errored group's response with the omitted sentinel, never with an
// evaluated bitfield, so a group carrying an error is left unknown.
func applyGroupAuthorizedOps(groups []metrics.GroupMetrics, described kadm.DescribedGroups) {
	for i := range groups {
		g, ok := described[groups[i].ID]
		groups[i].AuthorizedOperations = decodedAuthorizedOps(g.AuthorizedOperations, ok && g.Err == nil)
	}
}

// authorizedOps shapes one raw bitfield for the wire. The omitted sentinel
// becomes a nil *AuthorizedOps — "the broker did not say" — while a zero becomes
// a present, empty operation list — "the broker says nothing is permitted".
// Rendering the first as the second tells an operator their ACLs are gone when
// their broker is merely too old.
func authorizedOps(bitfield int32) *metrics.AuthorizedOps {
	if bitfield == authorizedOpsOmitted {
		return nil
	}
	return &metrics.AuthorizedOps{
		Bitfield:   &bitfield,
		Operations: operationNames(kadm.DecodeACLOperations(bitfield)),
	}
}

// decodedAuthorizedOps is authorizedOps for a bitfield kadm decoded before the
// agent saw it; reported is the caller's proof that the broker answered at all.
// Bitfield stays nil: the raw value is gone, and rebuilding it from the names
// would invent bits kadm dropped and claim they came off the wire.
//
// An empty decode is treated as "the broker did not say", not as "nothing is
// permitted", because kadm maps the omitted sentinel and a genuine zero to the
// same nil slice and the sentinel is unrecoverable by this point. Reading it the
// other way would be the worse error: a truly empty grant cannot reach here,
// since a principal with no operations on a group fails DescribeGroups with
// GROUP_AUTHORIZATION_FAILED and never produces a decoded entry at all. So an
// empty slice means an old broker, and calling that "no permissions" would tell
// an operator their ACLs are gone when only their broker is behind.
func decodedAuthorizedOps(ops []kadm.ACLOperation, reported bool) *metrics.AuthorizedOps {
	if !reported || len(ops) == 0 {
		return nil
	}
	return &metrics.AuthorizedOps{Operations: operationNames(ops)}
}

// operationNames renders ACL operations as the wire's sorted name list. It is
// never nil, so "nothing is permitted" marshals as [] and stays distinct from a
// null AuthorizedOps.
func operationNames(ops []kadm.ACLOperation) []string {
	names := make([]string, 0, len(ops))
	for _, op := range ops {
		names = append(names, op.String())
	}
	sort.Strings(names)
	return names
}
