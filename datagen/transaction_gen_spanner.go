package datagen

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/timer"
	"google.golang.org/api/iterator"
)

// TransactionGeneratorSpanner populates the transactions table within the ledger database.
type TransactionGeneratorSpanner struct {
	ctx     context.Context
	client  *spanner.Client
	rand    *rand.Rand
	rand2   *rand.Rand
	metrics *timer.Metrics
}

// NewTransactionGeneratorSpanner returns a new TransactionGeneratorSpanner instance.
func NewTransactionGeneratorSpanner(
	ctx context.Context,
	client *spanner.Client,
	metrics *timer.Metrics,
) *TransactionGeneratorSpanner {

	return &TransactionGeneratorSpanner{
		ctx:     ctx,
		client:  client,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
		rand2:   rand.New(rand.NewSource(time.Now().UnixNano())),
		metrics: metrics,
	}
}

// Generate adds a random list of transactions to the table.
//
// In a real-world application, constraint checks surrounding transactions would be very important,
// but for the purposes of performance evaluation, the transactions don't need to be strictly
// valid.
//
// If we randomly generate all of the transactions, across users, we expect the transactions to be
// distributed (somewhat) across users and companies. This is sufficient.
//
// In production, we may want to consider retrying when there is a conflicting ID.
//
// See the links below for more information.
//		https://cloud.google.com/spanner/docs/bulk-loading
func (gen *TransactionGeneratorSpanner) Generate() error {
	defer gen.metrics.Track(time.Now(), "TransactionGenerator.Generate")

	// For referential integrity, we still need to ensure that transactions select from a list of
	// valid company and user IDs.
	companyIDs, err := gen.queryIds(CompanyTableName)
	if err != nil {
		return err
	}
	userIDs, err := gen.queryIds(UserTableName)
	if err != nil {
		return err
	}

	// It's fine for us to generate fewer transactions to avoid partial buckets. When we're
	// collecting performance data, we want to make sure that every operation is identical.
	const bucketSize int64 = 3000
	const numBuckets int64 = TransactionCount / bucketSize

	for bucketIdx := int64(0); bucketIdx < numBuckets; bucketIdx++ {
		min := bucketSize * bucketIdx
		max := min + bucketSize
		if max > TransactionCount {
			max = TransactionCount
		}
		err := gen.generateForBucket(min, max, companyIDs, userIDs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (gen *TransactionGeneratorSpanner) queryIds(tableName string) ([]int64, error) {
	defer gen.metrics.Track(time.Now(), fmt.Sprintf("TransactionGenerator.queryIds[%s]", tableName))

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT Id FROM %s`, tableName),
	}
	start := time.Now()
	iter := gen.client.Single().Query(gen.ctx, stmt)
	gen.metrics.Track(start, fmt.Sprintf("TransactionGenerator.queryIds[%s].SQL", tableName))
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

func (gen *TransactionGeneratorSpanner) generateForBucket(
	min int64,
	max int64,
	companyIDs []int64,
	userIDs []int64,
) error {

	defer gen.metrics.Track(time.Now(), "TransactionGenerator.generateForBucket")

	// Define the allowable time range.
	const timeRange = TransactionMaxTime - TransactionMinTime

	// We use a monotonically incrementing ID here to optimize the performance on bulk insert. This
	// is normally a bad practice when you often query on the primary key, but because our primary
	// key is not semantically meaningful here (simply unique), this should allow for better data
	// locality without creating any hot spots.
	mutations := []*spanner.Mutation{}
	for i := min; i < max; i++ {
		companyIdx := gen.rand.Int31() % int32(len(companyIDs))
		companyID := companyIDs[companyIdx]

		fromUserIdx := gen.rand.Int31() % int32(len(userIDs))
		fromUserID := userIDs[fromUserIdx]

		toUserIdx := gen.rand.Int31() % int32(len(userIDs))
		toUserID := userIDs[toUserIdx]

		timeSec := gen.rand2.Int63()%timeRange + TransactionMinTime
		timeNanos := gen.rand2.Int63() % 1000000000

		mutation := spanner.InsertMap(TransactionTableName, map[string]interface{}{
			"id":         TransactionBaseID + i,
			"companyId":  companyID,
			"fromUserId": fromUserID,
			"toUserId":   toUserID,
			"time":       time.Unix(timeSec, timeNanos),
		})
		mutations = append(mutations, mutation)
	}
	start := time.Now()
	_, err := gen.client.Apply(gen.ctx, mutations)
	gen.metrics.Track(start, "TransactionGenerator.generateForBucket.SQL")
	return err
}
