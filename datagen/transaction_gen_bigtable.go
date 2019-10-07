package datagen

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/r7wang/gcloud-test/timer"
)

// TransactionGeneratorBigtable populates the transactions table within the ledger instance.
type TransactionGeneratorBigtable struct {
	ctx     context.Context
	client  *bigtable.Client
	rand    *rand.Rand
	rand2   *rand.Rand
	metrics *timer.Metrics
}

// NewTransactionGeneratorBigtable returns a new TransactionGeneratorBigtable instance.
func NewTransactionGeneratorBigtable(
	ctx context.Context,
	client *bigtable.Client,
	metrics *timer.Metrics,
) *TransactionGeneratorBigtable {

	return &TransactionGeneratorBigtable{
		ctx:     ctx,
		client:  client,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
		rand2:   rand.New(rand.NewSource(time.Now().UnixNano())),
		metrics: metrics,
	}
}

// Generate adds a random list of transactions to the table.
//
// If a row already has data for a given column and we happen to store an older timestamp of that
// data, Bigtable will still write that entry as part of a mutation operation. Querying for the
// latest copy of that data will not fetch the value from that older timestamp even though its
// insertion time was later.
//
// In this implementation, we are choosing to store foreign key references as strings instead of
// int64. One of the reasons is because those keys are also stored as strings. Bigtable
// documentation recommends the use of human-readable keys.
//
// See the links below for more information:
//		https://cloud.google.com/bigtable/docs/schema-design#types_of_row_keys
//
// TODO: Consider the use of export/import instead of writing a generator.
func (gen *TransactionGeneratorBigtable) Generate() error {
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

	const bucketSize int64 = 10000
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

func (gen *TransactionGeneratorBigtable) queryIds(tableName string) ([]string, error) {
	defer gen.metrics.Track(time.Now(), fmt.Sprintf("TransactionGenerator.queryIds[%s]", tableName))

	table := gen.client.Open(tableName)
	ids := []string{}
	err := table.ReadRows(gen.ctx, bigtable.PrefixRange(""), func(row bigtable.Row) bool {
		ids = append(ids, row.Key())
		return true
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (gen *TransactionGeneratorBigtable) generateForBucket(
	min int64,
	max int64,
	companyIDs []string,
	userIDs []string,
) error {

	defer gen.metrics.Track(time.Now(), "TransactionGenerator.generateForBucket")

	// Define the allowable time range.
	const timeRange = TransactionMaxTime - TransactionMinTime

	mutations := []*bigtable.Mutation{}
	rowKeys := []string{}
	for i := min; i < max; i++ {
		companyIdx := gen.rand.Int31() % int32(len(companyIDs))
		companyID := companyIDs[companyIdx]

		fromUserIdx := gen.rand.Int31() % int32(len(userIDs))
		fromUserID := userIDs[fromUserIdx]

		toUserIdx := gen.rand.Int31() % int32(len(userIDs))
		toUserID := userIDs[toUserIdx]

		timeSec := gen.rand2.Int63()%timeRange + TransactionMinTime
		timeNanos := gen.rand2.Int63() % 1000000000
		ts := bigtable.Time(time.Unix(timeSec, timeNanos))

		// Although unrealistic, it's probably sufficient to only use "second" granularity here.
		mutation := bigtable.NewMutation()
		mutation.Set(DefaultColumnFamily, TransactionCompanyColumn, ts, []byte(companyID))
		mutation.Set(DefaultColumnFamily, TransactionFromUserColumn, ts, []byte(fromUserID))
		mutation.Set(DefaultColumnFamily, TransactionToUserColumn, ts, []byte(toUserID))
		mutations = append(mutations, mutation)
		rowKeys = append(rowKeys, Int64String(TransactionBaseID+i))
	}
	table := gen.client.Open(TransactionTableName)
	if err := mergeErrors(table.ApplyBulk(gen.ctx, rowKeys, mutations)); err != nil {
		return err
	}
	return nil
}
