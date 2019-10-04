package workflow

import (
	"context"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/spanner/timer"
)

// OLTP defines operations to exercise common types of transactional workflows that define certain
// semantic guarantees.
type OLTP struct {
	ctx    context.Context
	client *spanner.Client
}

// NewOLTP reeturns a new OLTP instance.
func NewOLTP(ctx context.Context, client *spanner.Client) *OLTP {
	return &OLTP{ctx: ctx, client: client}
}

// Run sequentially executes all of the test workflows.
func (wf *OLTP) Run() error {
	r := rand.New(rand.NewSource(rand.Int63()))

	if err := wf.blindWrite(r); err != nil {
		return err
	}

	if err := wf.readAndUpdate(r); err != nil {
		return err
	}

	return nil
}

func (wf *OLTP) blindWrite(r *rand.Rand) error {
	const tableName = "Transactions"
	const numSamples = 1000

	for i := 0; i < numSamples; i++ {
		// For these tests, referential integrity is un-important since there are no defined
		// foreign key constraints.
		mutation := spanner.InsertMap(tableName, map[string]interface{}{
			"id":         r.Int63(),
			"companyId":  r.Int63(),
			"fromUserId": r.Int63(),
			"toUserId":   r.Int63(),
			"time":       spanner.CommitTimestamp,
		})
		start := time.Now()
		_, err := wf.client.Apply(wf.ctx, []*spanner.Mutation{mutation})
		timer.Track(start, "OLTP.blindWrite.SQL")
		if err != nil {
			return err
		}
	}

	return nil
}

func (wf *OLTP) readAndUpdate(r *rand.Rand) error {
	const tableName = "Transactions"
	const numSamples = 1000
	const numTransactions = 20000000
	const baseTransactionID int64 = 1000000000000000000

	for i := 0; i < numSamples; i++ {
		// This should be both valid and random, hence we need to know the range of valid
		// identifiers within the table.
		updateID := baseTransactionID + (r.Int63() % numTransactions)
		start := time.Now()
		_, err := wf.client.ReadWriteTransaction(wf.ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			row, err := txn.ReadRow(wf.ctx, tableName, spanner.Key{updateID}, []string{"fromUserId", "toUserId"})
			if err != nil {
				return err
			}
			var fromUserID, toUserID int64
			if err := row.Columns(&fromUserID, &toUserID); err != nil {
				return err
			}

			// Swapping the user IDs guarantees that referential integrity is maintained.
			mutation := spanner.UpdateMap(tableName, map[string]interface{}{
				"id":         updateID,
				"fromUserId": toUserID,
				"toUserId":   fromUserID,
				"time":       spanner.CommitTimestamp,
			})
			return txn.BufferWrite([]*spanner.Mutation{mutation})
		})
		timer.Track(start, "OLTP.readAndUpdate.SQL")
		if err != nil {
			return err
		}
	}

	return nil
}
