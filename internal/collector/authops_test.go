package collector

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"kafka-metrics-agent/internal/metrics"
)

// bits builds an authorized-operations bitfield the way Kafka does: bit N set
// for ACL operation code N.
func bits(ops ...kadm.ACLOperation) int32 {
	var b int32
	for _, op := range ops {
		b |= 1 << uint(op)
	}
	return b
}

func TestAuthorizedOpsSeparatesOmittedFromEmpty(t *testing.T) {
	// The whole point of the item: these two are opposite answers and both
	// decode to an empty operation list.
	if got := authorizedOps(authorizedOpsOmitted); got != nil {
		t.Errorf("omitted sentinel = %+v, want nil (the broker did not say)", got)
	}

	got := authorizedOps(0)
	if got == nil {
		t.Fatal("bitfield 0 = nil, want a present, empty grant (the broker said nothing is permitted)")
	}
	if got.Bitfield == nil || *got.Bitfield != 0 {
		t.Errorf("bitfield = %v, want 0", got.Bitfield)
	}
	if got.Operations == nil {
		t.Error("operations = nil, want an empty slice so it marshals as [] and not null")
	}
	if len(got.Operations) != 0 {
		t.Errorf("operations = %v, want none", got.Operations)
	}

	// The distinction has to survive JSON, since that is where a backend reads it.
	raw, err := json.Marshal(struct {
		Omitted *metrics.AuthorizedOps `json:"omitted"`
		Empty   *metrics.AuthorizedOps `json:"empty"`
	}{authorizedOps(authorizedOpsOmitted), got})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	const want = `{"omitted":null,"empty":{"bitfield":0,"operations":[]}}`
	if string(raw) != want {
		t.Errorf("json = %s, want %s", raw, want)
	}
}

func TestAuthorizedOpsDecodesAndSortsNames(t *testing.T) {
	got := authorizedOps(bits(kadm.OpDescribeConfigs, kadm.OpRead, kadm.OpDescribe))
	if got == nil {
		t.Fatal("authorizedOps = nil")
	}
	want := []string{"DESCRIBE", "DESCRIBE_CONFIGS", "READ"}
	if !reflect.DeepEqual(got.Operations, want) {
		t.Errorf("operations = %v, want %v", got.Operations, want)
	}
	// The raw bitfield is carried so a backend can recover bits kadm's decoder
	// does not know a name for.
	if got.Bitfield == nil || *got.Bitfield != bits(kadm.OpDescribeConfigs, kadm.OpRead, kadm.OpDescribe) {
		t.Errorf("bitfield = %v, want the raw value", got.Bitfield)
	}
}

func TestDecodedAuthorizedOpsNeedsProofTheBrokerAnswered(t *testing.T) {
	// kadm hands back the same nil slice for "omitted" and "nothing permitted",
	// so an unproven empty must stay unknown rather than become an empty grant.
	if got := decodedAuthorizedOps(nil, false); got != nil {
		t.Errorf("unreported = %+v, want nil", got)
	}
	// An empty decode stays UNKNOWN even when the group was described. The
	// sentinel is unrecoverable here, and of the two readings only this one can
	// be wrong safely: a principal with no operations on a group cannot reach
	// this code, because DescribeGroups would have failed with
	// GROUP_AUTHORIZATION_FAILED first. So empty means an old broker, and
	// calling it "no permissions" would tell an operator their ACLs are gone.
	if got := decodedAuthorizedOps(nil, true); got != nil {
		t.Errorf("reported but empty = %+v, want nil: an empty grant is unreachable here", got)
	}
	got := decodedAuthorizedOps([]kadm.ACLOperation{kadm.OpRead, kadm.OpDescribe}, true)
	// No raw value survived kadm's decode, and inventing one would claim it came
	// off the wire.
	if got.Bitfield != nil {
		t.Errorf("bitfield = %v, want nil for a decoded-only value", *got.Bitfield)
	}
	if want := []string{"DESCRIBE", "READ"}; !reflect.DeepEqual(got.Operations, want) {
		t.Errorf("operations = %v, want %v", got.Operations, want)
	}
}

func TestApplyGroupAuthorizedOps(t *testing.T) {
	groups := []metrics.GroupMetrics{{ID: "billing"}, {ID: "denied"}, {ID: "absent"}}
	described := kadm.DescribedGroups{
		"billing": {Group: "billing", AuthorizedOperations: []kadm.ACLOperation{kadm.OpDescribe, kadm.OpRead}},
		// An errored group carries the omitted sentinel, never an evaluated
		// bitfield: reporting "nothing permitted" here would be a lie about a
		// coordinator that simply refused to answer.
		"denied": {Group: "denied", Err: kerr.GroupAuthorizationFailed},
	}

	applyGroupAuthorizedOps(groups, described)

	if got := groups[0].AuthorizedOperations; got == nil || !reflect.DeepEqual(got.Operations, []string{"DESCRIBE", "READ"}) {
		t.Errorf("billing = %+v, want DESCRIBE and READ", got)
	}
	if got := groups[1].AuthorizedOperations; got != nil {
		t.Errorf("denied = %+v, want nil: an errored group's bitfield was never evaluated", got)
	}
	if got := groups[2].AuthorizedOperations; got != nil {
		t.Errorf("absent = %+v, want nil: the describe did not cover it", got)
	}
}

func TestAuthorizedOpsRequestNamesTopicsAndCreatesNothing(t *testing.T) {
	req := authorizedOpsRequest([]string{"orders", "payments"})
	if !req.IncludeTopicAuthorizedOperations || !req.IncludeClusterAuthorizedOperations {
		t.Error("both KIP-430 flags must be set; they are the only reason for this request")
	}
	if req.AllowAutoTopicCreation {
		t.Error("AllowAutoTopicCreation must stay false: a read-only agent must not recreate a deleted topic")
	}
	if len(req.Topics) != 2 || req.Topics[0].Topic == nil || *req.Topics[0].Topic != "orders" {
		t.Errorf("topics = %+v, want the two names", req.Topics)
	}

	// The trap: kmsg encodes a nil topics array as NULL and the broker reads
	// NULL as "every topic in the cluster", which would undo the agent's filter
	// and its cap.
	empty := authorizedOpsRequest(nil)
	if empty.Topics == nil {
		t.Fatal("an empty topic list must be non-nil, or the broker describes the whole cluster")
	}
	if len(empty.Topics) != 0 {
		t.Errorf("topics = %+v, want none", empty.Topics)
	}
}

// metadataResponse builds a Metadata response carrying one topic per name.
func metadataResponse(cluster int32, topics map[string]int32, errored map[string]int16) *kmsg.MetadataResponse {
	resp := kmsg.NewPtrMetadataResponse()
	resp.AuthorizedOperations = cluster
	for name, b := range topics {
		t := kmsg.NewMetadataResponseTopic()
		t.Topic = kmsg.StringPtr(name)
		t.AuthorizedOperations = b
		t.ErrorCode = errored[name]
		resp.Topics = append(resp.Topics, t)
	}
	return resp
}

func TestApplyTopicAuthorizedOps(t *testing.T) {
	topics := []metrics.TopicMetrics{{Name: "orders"}, {Name: "legacy"}, {Name: "vanished"}}
	resp := metadataResponse(0, map[string]int32{
		"orders": bits(kadm.OpDescribe, kadm.OpDescribeConfigs),
		// A broker below Metadata v8, or one that refused the topic, answers
		// with the omitted sentinel.
		"legacy": authorizedOpsOmitted,
	}, nil)

	sec := newSection(sectionAuthorizedOps)
	applyTopicAuthorizedOps(sec, topics, resp)

	if got := topics[0].AuthorizedOperations; got == nil || !reflect.DeepEqual(got.Operations, []string{"DESCRIBE", "DESCRIBE_CONFIGS"}) {
		t.Errorf("orders = %+v, want DESCRIBE and DESCRIBE_CONFIGS", got)
	}
	if got := topics[1].AuthorizedOperations; got != nil {
		t.Errorf("legacy = %+v, want nil for the omitted sentinel", got)
	}
	if got := topics[2].AuthorizedOperations; got != nil {
		t.Errorf("vanished = %+v, want nil", got)
	}
	// A topic the broker did not echo is an anomaly worth one error, not silence.
	if len(sec.errs) != 1 || sec.errs[0].Topic != "vanished" {
		t.Errorf("errors = %+v, want one naming vanished", sec.errs)
	}
	if sec.status != metrics.SectionPartial {
		t.Errorf("status = %q, want partial", sec.status)
	}
}

func TestApplyTopicAuthorizedOpsRecordsTopicErrors(t *testing.T) {
	topics := []metrics.TopicMetrics{{Name: "secret"}}
	resp := metadataResponse(0,
		map[string]int32{"secret": authorizedOpsOmitted},
		map[string]int16{"secret": kerr.TopicAuthorizationFailed.Code})

	sec := newSection(sectionAuthorizedOps)
	applyTopicAuthorizedOps(sec, topics, resp)

	if topics[0].AuthorizedOperations != nil {
		t.Errorf("secret = %+v, want nil: a refused topic's bitfield was never evaluated", topics[0].AuthorizedOperations)
	}
	if len(sec.errs) != 1 || sec.errs[0].Kind != kindAuthorization {
		t.Errorf("errors = %+v, want one authorization entry — the finding this phase exists for", sec.errs)
	}
}

func TestCollectAuthorizedOpsSkippedCycleIssuesNoRequest(t *testing.T) {
	// The nil client is the assertion: any path reaching the broker panics.
	c, err := New(nil, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	batch := &metrics.Batch{
		Topics: []metrics.TopicMetrics{{Name: "orders"}},
		Groups: []metrics.GroupMetrics{{ID: "billing"}},
	}
	described := kadm.DescribedGroups{
		"billing": {Group: "billing", AuthorizedOperations: []kadm.ACLOperation{kadm.OpDescribe}},
	}

	sec := c.collectAuthorizedOps(context.Background(), batch, described, false)

	if sec.status != metrics.SectionSkipped {
		t.Errorf("status = %q, want skipped", sec.status)
	}
	if len(sec.errs) != 0 {
		t.Errorf("a skipped section must record nothing, got %+v", sec.errs)
	}
	// Nothing may be stamped on a cycle that did not sample, or a backend cannot
	// tell an off cycle from a grant that disappeared.
	if batch.Topics[0].AuthorizedOperations != nil || batch.Groups[0].AuthorizedOperations != nil {
		t.Error("a skipped cycle must leave every authorized_operations null")
	}
	if batch.Cluster.AuthorizedOperations != nil {
		t.Error("a skipped cycle must leave the cluster grant null")
	}
}

func TestRunsThisCycle(t *testing.T) {
	tests := []struct {
		name    string
		enabled bool
		every   int
		n       uint64
		want    bool
	}{
		{name: "disabled", enabled: false, every: 1, n: 0},
		{name: "first cycle samples", enabled: true, every: 10, n: 0, want: true},
		{name: "between samples", enabled: true, every: 10, n: 3},
		{name: "next sample", enabled: true, every: 10, n: 10, want: true},
		{name: "zero cadence means every cycle", enabled: true, every: 0, n: 7, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runsThisCycle(tt.enabled, tt.every, tt.n); got != tt.want {
				t.Errorf("runsThisCycle(%t, %d, %d) = %t, want %t", tt.enabled, tt.every, tt.n, got, tt.want)
			}
		})
	}
}
