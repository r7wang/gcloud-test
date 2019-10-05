package workflow

import (
	"context"
	"math/rand"

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
	if err := wf.runner.runTest(wf.simpleRandomReadRow, "OLTP.simpleRandomReadRow"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.multiSequentialRead, "OLTP.multiSequentialRead"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.multiRandomRead, "OLTP.multiRandomRead"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.readAndUpdate, "OLTP.readAndUpdate"); err != nil {
		return err
	}
	keys, err := wf.runner.runTestReturns(wf.blindWrite, "OLTP.blindWrite")
	if err != nil {
		return err
	}
	if err := wf.runner.runTestWith(wf.delete, keys, "OLTP.delete"); err != nil {
		return err
	}
	return nil
}

func (wf *OLTPBigtable) simpleRandomReadRow(r *rand.Rand) error {
	return nil
}

func (wf *OLTPBigtable) multiSequentialRead(r *rand.Rand) error {
	return nil
}

func (wf *OLTPBigtable) multiRandomRead(r *rand.Rand) error {
	return nil
}

func (wf *OLTPBigtable) readAndUpdate(r *rand.Rand) error {
	return nil
}

func (wf *OLTPBigtable) blindWrite(r *rand.Rand) (int64, error) {
	return 0, nil
}

func (wf *OLTPBigtable) delete(r *rand.Rand, key int64) error {
	return nil
}
