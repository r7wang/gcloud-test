package workflow

import (
	"context"

	"cloud.google.com/go/bigtable"
)

// OLTPBigtable defines operations to exercise common types of transactional workflows with certain
// semantic guarantees.
type OLTPBigtable struct {
	ctx    context.Context
	runner *runner
	client *bigtable.Client
}

// NewOLTPBigtable returns a new OLTPBigtable instance.
func NewOLTPBigtable(ctx context.Context, client *bigtable.Client) *OLTPBigtable {
	return &OLTPBigtable{ctx: ctx, runner: &runner{}, client: client}
}

// Run sequentially executes all of the test workflows.
func (wf *OLTPBigtable) Run() error {
	return nil
}
