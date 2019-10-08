package workflow

import (
	"context"
	"fmt"
	"math/rand"

	"cloud.google.com/go/bigtable"
	"github.com/r7wang/gcloud-test/datagen"
	"github.com/r7wang/gcloud-test/timer"
)

// OLAPBigtable defines operations to exercise common types of analytical workflows across large
// chunks of data.
type OLAPBigtable struct {
	ctx     context.Context
	runner  *runner
	client  *bigtable.Client
	metrics *timer.Metrics
}

// NewOLAPBigtable returns a new OLAPBigtable instance.
func NewOLAPBigtable(
	ctx context.Context,
	client *bigtable.Client,
	metrics *timer.Metrics,
) *OLAPBigtable {

	return &OLAPBigtable{
		ctx:     ctx,
		runner:  newRunner(metrics),
		client:  client,
		metrics: metrics,
	}
}

// Run sequentially executes all of the test workflows.
func (wf *OLAPBigtable) Run() error {
	if err := wf.runner.runTest(wf.simpleTopN, "OLAP.simpleTopN"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.aggregationTopN, "OLAP.aggregationTopN"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.targetedOrderedScan, "OLAP.targetedOrderedScan"); err != nil {
		return err
	}
	return nil
}

func (wf *OLAPBigtable) simpleTopN(r *rand.Rand) error {
	/*
		table := wf.client.Open(datagen.TransactionTableName)
		rowRange := bigtable.RowList(readIDs)
		if err := table.ReadRows(wf.ctx, rowRange, wf.scanRow); err != nil {
			return err
		}
	*/
	return nil
}

func (wf *OLAPBigtable) aggregationTopN(r *rand.Rand) error {
	return nil
}

func (wf *OLAPBigtable) targetedOrderedScan(r *rand.Rand) error {
	return nil
}

func (wf *OLAPBigtable) scanRow(row bigtable.Row) bool {
	cf := row[datagen.DefaultColumnFamily]
	for _, col := range cf {
		if col.Column == fmt.Sprintf("%s:%s", datagen.DefaultColumnFamily, datagen.TransactionFromUserColumn) {
			continue
		}
		if col.Column == fmt.Sprintf("%s:%s", datagen.DefaultColumnFamily, datagen.TransactionToUserColumn) {
			continue
		}
	}
	return true
}
