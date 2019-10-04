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
	ctx    context.Context
	client *spanner.Client
}

// NewTransactionGeneratorSpanner returns a new TransactionGeneratorSpanner instance.
func NewTransactionGeneratorSpanner(ctx context.Context, client *spanner.Client) *TransactionGeneratorSpanner {
	return &TransactionGeneratorSpanner{ctx: ctx, client: client}
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
	defer timer.Track(time.Now(), "TransactionGenerator.Generate")

	// For referential integrity, we still need to ensure that transactions select from a list of
	// valid company and user IDs.
	companyIDs, err := gen.queryIds("Companies")
	if err != nil {
		return err
	}
	userIDs, err := gen.queryIds("Users")
	if err != nil {
		return err
	}

	// It's fine for us to generate fewer transactions to avoid partial buckets. When we're
	// collecting performance data, we want to make sure that every operation is identical.
	const numTransactions = 20000000
	const bucketSize = 3000
	const numBuckets = numTransactions / bucketSize

	for bucketIdx := 0; bucketIdx < numBuckets; bucketIdx++ {
		min := bucketSize * bucketIdx
		max := min + bucketSize
		if max > numTransactions {
			max = numTransactions
		}
		err := gen.generateForBucket(min, max, companyIDs, userIDs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (gen *TransactionGeneratorSpanner) queryIds(tableName string) ([]int64, error) {
	defer timer.Track(time.Now(), fmt.Sprintf("TransactionGenerator.queryIds[%s]", tableName))

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT Id FROM %s`, tableName),
	}
	start := time.Now()
	iter := gen.client.Single().Query(gen.ctx, stmt)
	timer.Track(start, fmt.Sprintf("TransactionGenerator.queryIds[%s].SQL", tableName))
	defer iter.Stop()
	companyIDs := []int64{}
	var companyID int64
	for {
		row, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		if err := row.Columns(&companyID); err != nil {
			return nil, err
		}
		companyIDs = append(companyIDs, companyID)
	}
	return companyIDs, nil
}

func (gen *TransactionGeneratorSpanner) generateForBucket(
	min int,
	max int,
	companyIDs []int64,
	userIDs []int64,
) error {

	defer timer.Track(time.Now(), "TransactionGenerator.generateForBucket")

	const tableName = "Transactions"
	const baseTransactionID int64 = 1000000000000000000

	// Define the allowable time range.
	const minTime int64 = 1451606400 // 2016-01-01
	const maxTime int64 = 1567296000 // 2019-09-01
	const timeRange = maxTime - minTime

	// We use a monotonically incrementing ID here to optimize the performance on bulk insert. This
	// is normally a bad practice when you often query on the primary key, but because our primary
	// key is not semantically meaningful here (simply unique), this should allow for better data
	// locality without creating any hot spots.
	mutations := []*spanner.Mutation{}
	for i := min; i < max; i++ {
		companyIdx := rand.Int31() % int32(len(companyIDs))
		companyID := companyIDs[companyIdx]

		fromUserIdx := rand.Int31() % int32(len(userIDs))
		fromUserID := userIDs[fromUserIdx]

		toUserIdx := rand.Int31() % int32(len(userIDs))
		toUserID := userIDs[toUserIdx]

		unixTime := rand.Int63()%timeRange + minTime

		// Although unrealistic, it's probably sufficient to only use "second" granularity here.
		mutation := spanner.InsertMap(tableName, map[string]interface{}{
			"id":         baseTransactionID + int64(i),
			"companyId":  companyID,
			"fromUserId": fromUserID,
			"toUserId":   toUserID,
			"time":       time.Unix(unixTime, 0),
		})
		mutations = append(mutations, mutation)
	}
	start := time.Now()
	_, err := gen.client.Apply(gen.ctx, mutations)
	timer.Track(start, "TransactionGenerator.generateForBucket.SQL")
	return err
}
