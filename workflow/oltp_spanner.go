package workflow

import (
	"context"
	"math/rand"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/datagen"
	"github.com/r7wang/gcloud-test/timer"
	"google.golang.org/api/iterator"
)

// OLTPSpanner defines operations to exercise common types of transactional workflows with certain
// semantic guarantees.
type OLTPSpanner struct {
	ctx    context.Context
	runner *runner
	client *spanner.Client
}

// NewOLTPSpanner returns a new OLTPSpanner instance.
func NewOLTPSpanner(
	ctx context.Context,
	client *spanner.Client,
	metrics *timer.Metrics,
) *OLTPSpanner {

	return &OLTPSpanner{
		ctx:    ctx,
		runner: newRunner(metrics),
		client: client}
}

// Run sequentially executes all of the test workflows.
//
// Consider adding a multiple random read test that uses a SQL query.
func (wf *OLTPSpanner) Run() error {
	if err := wf.runner.runTest(wf.simpleRandomReadRow, "OLTP.simpleRandomReadRow"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.simpleRandomQuery, "OLTP.simpleRandomQuery"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.multiSequentialRead, "OLTP.multiSequentialRead"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.multiRandomRead, "OLTP.multiRandomRead"); err != nil {
		return err
	}
	if err := wf.runner.runTest(wf.atomicSwap, "OLTP.atomicSwap"); err != nil {
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

// Read a single row using ReadRow.
func (wf *OLTPSpanner) simpleRandomReadRow(r *rand.Rand) error {
	readID := datagen.RandomGeneratedTransactionID(r)
	row, err := wf.client.Single().ReadRow(
		wf.ctx,
		datagen.TransactionTableName,
		spanner.Key{readID},
		[]string{"fromUserId", "toUserId"})
	if err != nil {
		return err
	}
	var fromUserID, toUserID int64
	if err := row.Columns(&fromUserID, &toUserID); err != nil {
		return err
	}
	return nil
}

// Read a single row using the Query and DML.
func (wf *OLTPSpanner) simpleRandomQuery(r *rand.Rand) error {
	readID := datagen.RandomGeneratedTransactionID(r)
	stmt := spanner.Statement{
		SQL: `SELECT t.FromUserId, t.ToUserId
				FROM Transactions t
				WHERE t.Id = @id`,
		Params: map[string]interface{}{
			"id": readID,
		},
	}
	iter := wf.client.Single().Query(wf.ctx, stmt)
	defer iter.Stop()
	if err := wf.scanIterator(iter); err != nil {
		return err
	}
	return nil
}

// Read multiple rows using a sequential Read.
func (wf *OLTPSpanner) multiSequentialRead(r *rand.Rand) error {
	const numReads = 100

	startReadID, endReadID := datagen.RandomGeneratedTransactionIDRange(r, numReads)
	iter := wf.client.Single().Read(
		wf.ctx,
		datagen.TransactionTableName,
		spanner.KeyRange{
			Start: spanner.Key{startReadID},
			End:   spanner.Key{endReadID},
		},
		[]string{"fromUserId", "toUserId"})
	defer iter.Stop()
	if err := wf.scanIterator(iter); err != nil {
		return err
	}
	return nil
}

// Read multiple rows using a random Read.
func (wf *OLTPSpanner) multiRandomRead(r *rand.Rand) error {
	const numReads = 5

	txn := wf.client.ReadOnlyTransaction()
	defer txn.Close()
	for i := 0; i < numReads; i++ {
		readID := datagen.RandomGeneratedTransactionID(r)
		row, err := txn.ReadRow(
			wf.ctx,
			datagen.TransactionTableName,
			spanner.Key{readID},
			[]string{"fromUserId", "toUserId"})
		if err != nil {
			return err
		}
		var fromUserID, toUserID int64
		if err := row.Columns(&fromUserID, &toUserID); err != nil {
			return err
		}
	}
	return nil
}

// Read and update a single row.
func (wf *OLTPSpanner) atomicSwap(r *rand.Rand) error {
	// This should be both valid and random, hence we need to know the range of valid
	// identifiers within the table.
	updateID := datagen.RandomGeneratedTransactionID(r)
	_, err := wf.client.ReadWriteTransaction(wf.ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(
			wf.ctx,
			datagen.TransactionTableName,
			spanner.Key{updateID},
			[]string{"fromUserId", "toUserId"})
		if err != nil {
			return err
		}
		var fromUserID, toUserID int64
		if err := row.Columns(&fromUserID, &toUserID); err != nil {
			return err
		}

		// Swapping the user IDs guarantees that referential integrity is maintained.
		mutation := spanner.UpdateMap(datagen.TransactionTableName, map[string]interface{}{
			"id":         updateID,
			"fromUserId": toUserID,
			"toUserId":   fromUserID,
			"time":       spanner.CommitTimestamp,
		})
		return txn.BufferWrite([]*spanner.Mutation{mutation})
	})
	if err != nil {
		return err
	}
	return nil
}

// Blindly write a single row.
func (wf *OLTPSpanner) blindWrite(r *rand.Rand) (int64, error) {
	// For these tests, referential integrity is un-important since there are no defined
	// foreign key constraints.
	addID := r.Int63()
	mutation := spanner.InsertMap(datagen.TransactionTableName, map[string]interface{}{
		"id":         addID,
		"companyId":  r.Int63(),
		"fromUserId": r.Int63(),
		"toUserId":   r.Int63(),
		"time":       spanner.CommitTimestamp,
	})
	_, err := wf.client.Apply(wf.ctx, []*spanner.Mutation{mutation})
	if err != nil {
		return 0, err
	}
	return addID, nil
}

// Delete a predefined row.
func (wf *OLTPSpanner) delete(r *rand.Rand, key int64) error {
	mutation := spanner.Delete(datagen.TransactionTableName, spanner.Key{key})
	_, err := wf.client.Apply(wf.ctx, []*spanner.Mutation{mutation})
	if err != nil {
		return err
	}
	return nil
}

func (wf *OLTPSpanner) scanIterator(iter *spanner.RowIterator) error {
	var fromUserID, toUserID int64
	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		if err := row.Columns(&fromUserID, &toUserID); err != nil {
			return err
		}
	}
	return nil
}
