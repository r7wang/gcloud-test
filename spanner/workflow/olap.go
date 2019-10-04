package workflow

import (
	"context"

	"cloud.google.com/go/spanner"
)

// OLAP defines operations to exercise common types of analytical workflows across large chunks of
// data.
type OLAP struct {
	ctx    context.Context
	client *spanner.Client
}

// NewOLAP returns a new OLAP instance.
func NewOLAP(ctx context.Context, client *spanner.Client) *OLAP {
	return &OLAP{ctx: ctx, client: client}
}

// Run - TODO
func (wf *OLAP) Run() error {
	return nil
}
