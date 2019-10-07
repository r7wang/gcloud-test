package workflow

import (
	"context"
	"fmt"
	"math/rand"

	"cloud.google.com/go/bigtable"
	"github.com/r7wang/gcloud-test/datagen"
	"github.com/r7wang/gcloud-test/timer"
)

// OLTPBigtable defines operations to exercise common types of transactional workflows with certain
// semantic guarantees.
type OLTPBigtable struct {
	ctx     context.Context
	runner  *runner
	client  *bigtable.Client
	metrics *timer.Metrics
}

// NewOLTPBigtable returns a new OLTPBigtable instance.
func NewOLTPBigtable(
	ctx context.Context,
	client *bigtable.Client,
	metrics *timer.Metrics,
) *OLTPBigtable {

	return &OLTPBigtable{
		ctx:     ctx,
		runner:  newRunner(metrics),
		client:  client,
		metrics: metrics,
	}
}

// Run sequentially executes all of the test workflows.
//
// Bigtable does not support atomically swapping data within two columns of a single row. Consider
// adding tests for atomicIncrement, conditionalWrite.
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

	if err := wf.runner.runTest(wf.atomicAppend, "OLTP.atomicAppend"); err != nil {
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
	readID := datagen.RandomGeneratedTransactionIDString(r)
	table := wf.client.Open(datagen.TransactionTableName)
	row, err := table.ReadRow(wf.ctx, readID)
	if err != nil {
		return err
	}
	wf.scanRow(row)
	return nil
}

func (wf *OLTPBigtable) multiSequentialRead(r *rand.Rand) error {
	const numReads = 100

	startReadID, endReadID := datagen.RandomGeneratedTransactionIDStringRange(r, numReads)
	table := wf.client.Open(datagen.TransactionTableName)
	rowRange := bigtable.NewRange(startReadID, endReadID)
	if err := table.ReadRows(wf.ctx, rowRange, wf.scanRow); err != nil {
		return err
	}
	return nil
}

func (wf *OLTPBigtable) multiRandomRead(r *rand.Rand) error {
	const numReads = 5

	readIDs := []string{}
	for i := 0; i < numReads; i++ {
		readID := datagen.RandomGeneratedTransactionIDString(r)
		readIDs = append(readIDs, readID)
	}
	table := wf.client.Open(datagen.TransactionTableName)
	rowRange := bigtable.RowList(readIDs)
	if err := table.ReadRows(wf.ctx, rowRange, wf.scanRow); err != nil {
		return err
	}
	return nil
}

func (wf *OLTPBigtable) atomicAppend(r *rand.Rand) error {
	readID := datagen.RandomGeneratedTransactionIDString(r)
	rw := bigtable.NewReadModifyWrite()
	rw.AppendValue(datagen.DefaultColumnFamily, datagen.TransactionToUserColumn, []byte("-test"))
	table := wf.client.Open(datagen.TransactionTableName)
	row, err := table.ApplyReadModifyWrite(wf.ctx, readID, rw)
	if err != nil {
		return err
	}
	wf.scanRow(row)
	return nil
}

func (wf *OLTPBigtable) blindWrite(r *rand.Rand) (int64, error) {
	// For these tests, referential integrity is un-important since there are no defined
	// foreign key constraints.
	addID := r.Int63()
	ts := bigtable.Now()
	mutation := bigtable.NewMutation()
	mutation.Set(datagen.DefaultColumnFamily, datagen.TransactionCompanyColumn, ts, []byte(datagen.Int64String(r.Int63())))
	mutation.Set(datagen.DefaultColumnFamily, datagen.TransactionFromUserColumn, ts, []byte(datagen.Int64String(r.Int63())))
	mutation.Set(datagen.DefaultColumnFamily, datagen.TransactionToUserColumn, ts, []byte(datagen.Int64String(r.Int63())))
	table := wf.client.Open(datagen.TransactionTableName)
	if err := table.Apply(wf.ctx, datagen.Int64String(addID), mutation); err != nil {
		return 0, err
	}
	return addID, nil
}

func (wf *OLTPBigtable) delete(r *rand.Rand, key int64) error {
	mutation := bigtable.NewMutation()
	mutation.DeleteRow()
	table := wf.client.Open(datagen.TransactionTableName)
	if err := table.Apply(wf.ctx, datagen.Int64String(key), mutation); err != nil {
		return err
	}
	return nil
}

func (wf *OLTPBigtable) scanRow(row bigtable.Row) bool {
	var fromUserID, toUserID string
	cf := row[datagen.DefaultColumnFamily]
	for _, col := range cf {
		if col.Column == fmt.Sprintf("%s:%s", datagen.DefaultColumnFamily, datagen.TransactionFromUserColumn) {
			fromUserID = string(col.Value)
			continue
		}
		if col.Column == fmt.Sprintf("%s:%s", datagen.DefaultColumnFamily, datagen.TransactionToUserColumn) {
			toUserID = string(col.Value)
			continue
		}
	}
	fmt.Println(fromUserID, toUserID)
	return true
}
