package datagen

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/spanner/timer"
	"google.golang.org/api/iterator"
)

// TransactionGenerator populates the transactions table within the ledger database.
type TransactionGenerator struct {
	client *spanner.Client
}

// NewTransactionGenerator returns a new TransactionGenerator instance.
func NewTransactionGenerator(client *spanner.Client) *TransactionGenerator {
	return &TransactionGenerator{client: client}
}

// Generate adds a random list of users to the table.
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
func (gen *TransactionGenerator) Generate() error {
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

	// Randomly search for 1 company, a from user and a to user.
	const numTransactions = 20000000
	const bucketSize = 3000
	const numBuckets = numTransactions / bucketSize

	for bucketIdx := 0; bucketIdx < numBuckets; bucketIdx++ {
		err := gen.generateForBucket(bucketSize, companyIDs, userIDs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (gen *TransactionGenerator) queryIds(tableName string) ([]int64, error) {
	defer timer.Track(time.Now(), fmt.Sprintf("TransactionGenerator.queryIds-%s", tableName))

	stmt := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT Id FROM %s`, tableName),
	}
	iter := gen.client.Single().Query(context.Background(), stmt)
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

func (gen *TransactionGenerator) generateForBucket(
	bucketSize int,
	companyIDs []int64,
	userIDs []int64,
) error {

	defer timer.Track(time.Now(), "TransactionGenerator.generateForBucket")

	const tableName = "Transactions"

	// Randomly seeding the RNG gives us a better chance to avoid collisions, especially when
	// having to retry failed inserts by rerunning the transaction generator.
	r := rand.New(rand.NewSource(rand.Int63()))
	mutations := []*spanner.Mutation{}
	for i := 0; i < bucketSize; i++ {
		companyIdx := rand.Int31() % int32(len(companyIDs))
		companyID := companyIDs[companyIdx]

		fromUserIdx := rand.Int31() % int32(len(userIDs))
		fromUserID := userIDs[fromUserIdx]

		toUserIdx := rand.Int31() % int32(len(userIDs))
		toUserID := userIDs[toUserIdx]

		mutation := spanner.InsertMap(tableName, map[string]interface{}{
			"id":         r.Int63(),
			"companyId":  companyID,
			"fromUserId": fromUserID,
			"toUserId":   toUserID,
			"time":       spanner.CommitTimestamp,
		})
		mutations = append(mutations, mutation)
	}
	_, err := gen.client.Apply(context.Background(), mutations)
	return err
}
