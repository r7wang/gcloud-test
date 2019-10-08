package workflow

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/datagen"
	"github.com/r7wang/gcloud-test/timer"
	"google.golang.org/api/iterator"
)

// OLAPSpanner defines operations to exercise common types of analytical workflows across large
// chunks of data.
type OLAPSpanner struct {
	ctx     context.Context
	runner  *runner
	client  *spanner.Client
	metrics *timer.Metrics
}

// NewOLAPSpanner returns a new OLAPSpanner instance.
func NewOLAPSpanner(
	ctx context.Context,
	client *spanner.Client,
	metrics *timer.Metrics,
) *OLAPSpanner {

	return &OLAPSpanner{
		ctx:     ctx,
		runner:  newRunner(metrics),
		client:  client,
		metrics: metrics,
	}
}

// Run sequentially executes all of the test workflows.
//
// TODO: We currently have a query that finds the top transaction volume months (overall), but we
//       may want to convert that into top 10 highest transaction volume months, which adds
//       slightly more complex year/month extraction.
// TODO: Is it worth considering an aggregation query that does not involve extraction?
// TODO: We should consider adding a join heavy example where we deal with properties of various
//       entities. For example, different types of users transacting to each other, where the
//       transaction is a certain type of transaction.
func (wf *OLAPSpanner) Run() error {
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

func (wf *OLAPSpanner) simpleTopN(r *rand.Rand) error {
	stmt := spanner.Statement{
		SQL: `SELECT t.Time
				FROM Transactions t
				ORDER BY t.Time DESC
				LIMIT 100`,
	}
	iter := wf.client.Single().Query(wf.ctx, stmt)
	defer iter.Stop()
	if err := wf.scanIteratorTime(iter); err != nil {
		return err
	}
	return nil
}

func (wf *OLAPSpanner) aggregationTopN(r *rand.Rand) error {
	stmt := spanner.Statement{
		SQL: `SELECT agg.Month, agg.TransactionCount
				FROM
				(
					SELECT EXTRACT(MONTH FROM t.Time) AS Month, COUNT(t.Id) AS TransactionCount
					FROM Transactions t
					GROUP BY EXTRACT(MONTH FROM t.Time)
				) agg
				ORDER BY agg.TransactionCount DESC`,
	}
	iter := wf.client.Single().Query(wf.ctx, stmt)
	defer iter.Stop()
	if err := wf.scanIteratorMonthCount(iter); err != nil {
		return err
	}
	return nil
}

// TODO: The way this test is written is not optimal. It actually requires an ID as input, hence
//       there are two queries within one test.
func (wf *OLAPSpanner) targetedOrderedScan(r *rand.Rand) error {
	userIDs, err := wf.queryIds(datagen.UserTableName)
	if err != nil {
		return err
	}
	readIdx := r.Int31() % int32(len(userIDs))
	readID := userIDs[readIdx]

	start := time.Now()
	stmt := spanner.Statement{
		SQL: `SELECT t.Time
				FROM Transactions t
				WHERE t.FromUserId = @id
				ORDER BY t.Time DESC`,
		Params: map[string]interface{}{
			"id": readID,
		},
	}
	iter := wf.client.Single().Query(wf.ctx, stmt)
	defer iter.Stop()
	if err := wf.scanIteratorTime(iter); err != nil {
		return err
	}
	wf.metrics.Track(start, "OLAP.targetedOrderedScan.SQL")
	return nil
}

func (wf *OLAPSpanner) queryIds(tableName string) ([]int64, error) {
	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT Id FROM %s`, tableName),
	}
	iter := wf.client.Single().Query(wf.ctx, stmt)
	defer iter.Stop()
	ids := []int64{}
	var id int64
	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		if err := row.Columns(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (wf *OLAPSpanner) scanIteratorTime(iter *spanner.RowIterator) error {
	var time time.Time
	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		if err := row.Columns(&time); err != nil {
			return err
		}
	}
	return nil
}

func (wf *OLAPSpanner) scanIteratorMonthCount(iter *spanner.RowIterator) error {
	var month, count int64
	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		if err := row.Columns(&month, &count); err != nil {
			return err
		}
	}
	return nil
}
