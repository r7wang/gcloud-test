package workflow

import (
	"context"
	"math/rand"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// OLTP defines operations to exercise common types of transactional workflows that define certain
// semantic guarantees.
type OLTP struct {
	ctx    context.Context
	runner *runner
	client *spanner.Client
}

// NewOLTP reeturns a new OLTP instance.
func NewOLTP(ctx context.Context, client *spanner.Client) *OLTP {
	return &OLTP{ctx: ctx, runner: &runner{}, client: client}
}

// Run sequentially executes all of the test workflows.
func (wf *OLTP) Run() error {
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

// Read a single row using ReadRow.
func (wf *OLTP) simpleRandomReadRow(r *rand.Rand) error {
	const numTransactions = 20000000
	const baseTransactionID int64 = 1000000000000000000

	readID := baseTransactionID + (r.Int63() % numTransactions)
	row, err := wf.client.Single().ReadRow(
		wf.ctx,
		TransactionTableName,
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
func (wf *OLTP) simpleRandomQuery(r *rand.Rand) error {
	const numTransactions = 20000000
	const baseTransactionID int64 = 1000000000000000000

	readID := baseTransactionID + (r.Int63() % numTransactions)
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
	var fromUserID, toUserID int64
	if err := wf.scanIterator(iter, fromUserID, toUserID); err != nil {
		return err
	}
	return nil
}

// Read multiple (5) rows using a sequential Read.
func (wf *OLTP) multiSequentialRead(r *rand.Rand) error {
	const numTransactions = 20000000
	const baseTransactionID int64 = 1000000000000000000
	const numReads = 5

	startReadID := baseTransactionID + (r.Int63() % numTransactions)
	iter := wf.client.Single().Read(
		wf.ctx,
		TransactionTableName,
		spanner.KeyRange{
			Start: spanner.Key{startReadID},
			End:   spanner.Key{startReadID + numReads},
		},
		[]string{"fromUserId", "toUserId"})
	defer iter.Stop()
	var fromUserID, toUserID int64
	if err := wf.scanIterator(iter, fromUserID, toUserID); err != nil {
		return err
	}
	return nil
}

// Read multiple (5) rows using a random Read.
func (wf *OLTP) multiRandomRead(r *rand.Rand) error {
	const numTransactions = 20000000
	const baseTransactionID int64 = 1000000000000000000
	const numReads = 5

	txn := wf.client.ReadOnlyTransaction()
	defer txn.Close()
	for i := 0; i < numReads; i++ {
		readID := baseTransactionID + (r.Int63() % numTransactions)
		row, err := txn.ReadRow(
			wf.ctx,
			TransactionTableName,
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
func (wf *OLTP) readAndUpdate(r *rand.Rand) error {
	const numTransactions = 20000000
	const baseTransactionID int64 = 1000000000000000000

	// This should be both valid and random, hence we need to know the range of valid
	// identifiers within the table.
	updateID := baseTransactionID + (r.Int63() % numTransactions)
	_, err := wf.client.ReadWriteTransaction(wf.ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(
			wf.ctx,
			TransactionTableName,
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
		mutation := spanner.UpdateMap(TransactionTableName, map[string]interface{}{
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
func (wf *OLTP) blindWrite(r *rand.Rand) (int64, error) {
	// For these tests, referential integrity is un-important since there are no defined
	// foreign key constraints.
	addID := r.Int63()
	mutation := spanner.InsertMap(TransactionTableName, map[string]interface{}{
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
func (wf *OLTP) delete(r *rand.Rand, key int64) error {
	mutation := spanner.Delete(TransactionTableName, spanner.Key{key})
	_, err := wf.client.Apply(wf.ctx, []*spanner.Mutation{mutation})
	if err != nil {
		return err
	}
	return nil
}

func (wf *OLTP) scanIterator(iter *spanner.RowIterator, collectors ...interface{}) error {
	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		if err := row.Columns(collectors); err != nil {
			return err
		}
	}
	return nil
}
