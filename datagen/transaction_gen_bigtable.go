package datagen

import (
	"context"
	"math/rand"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/r7wang/gcloud-test/timer"
)

// TransactionGeneratorBigtable populates the transactions table within the ledger instance.
type TransactionGeneratorBigtable struct {
	ctx    context.Context
	client *bigtable.Client
}

// NewTransactionGeneratorBigtable returns a new TransactionGeneratorBigtable instance.
func NewTransactionGeneratorBigtable(ctx context.Context, client *bigtable.Client) *TransactionGeneratorBigtable {
	return &TransactionGeneratorBigtable{ctx: ctx, client: client}
}

// Generate adds a random list of transactions to the table.
//
// TODO: This should probably be reworked so that company and users are no longer randomly
//		 generated IDs in transactions.
//
// TODO: Consider the use of export/import instead of writing a generator.
//
// TODO: Consider passing a seeded rand as a service. This seems to get more randomized results.
func (gen *TransactionGeneratorBigtable) Generate() error {
	defer timer.Track(time.Now(), "TransactionGenerator.Generate")

	// For referential integrity, we still need to ensure that transactions select from a list of
	// valid company and user IDs.

	table := gen.client.Open(TransactionTableName)
	mutation := gen.getMutation(
		TransactionCompanyColumn,
		TransactionFromUserColumn,
		TransactionToUserColumn)
	if err := table.Apply(gen.ctx, int64String(TransactionBaseID), mutation); err != nil {
		return err
	}
	return nil
}

// Returns a mutation that sets random IDs on a set of columns.
func (gen *TransactionGeneratorBigtable) getMutation(colNames ...string) *bigtable.Mutation {
	// Define the allowable time range.
	const timeRange = TransactionMaxTime - TransactionMinTime

	randSeeded := rand.New(rand.NewSource(rand.Int63()))
	mutation := bigtable.NewMutation()
	unixTime := randSeeded.Int63()%timeRange + TransactionMinTime
	ts := bigtable.Time(time.Unix(unixTime, 0))
	for _, colName := range colNames {
		mutation.Set(DefaultColumnFamily, colName, ts, gen.randomID())
	}
	return mutation
}

// BigEndian is used here to simulate how an int64 might normally be stored.
func (gen *TransactionGeneratorBigtable) randomID() []byte {
	val := rand.Int63()
	return int64Bytes(val)
}
