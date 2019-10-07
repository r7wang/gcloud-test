package workflow

import (
	"context"

	"cloud.google.com/go/spanner"
)

// OLAPSpanner defines operations to exercise common types of analytical workflows across large chunks of
// data.
type OLAPSpanner struct {
	ctx    context.Context
	client *spanner.Client
}

// NewOLAPSpanner returns a new OLAPSpanner instance.
func NewOLAPSpanner(ctx context.Context, client *spanner.Client) *OLAPSpanner {
	return &OLAPSpanner{ctx: ctx, client: client}
}

// Run - TODO
func (wf *OLAPSpanner) Run() error {
	return nil
}
