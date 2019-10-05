package datagen

import (
	"context"
	"encoding/binary"
	"math/rand"
	"time"

	"cloud.google.com/go/bigtable"
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
func (gen *TransactionGeneratorBigtable) Generate() error {
	table := gen.client.Open(TransactionTableName)
	mutation := gen.getMutation(
		TransactionCompanyColumn,
		TransactionFromUserColumn,
		TransactionToUserColumn)
	if err := table.Apply(gen.ctx, string(TransactionBaseID), mutation); err != nil {
		return err
	}
	return nil
}

// Returns a mutation that sets random IDs on a set of columns.
func (gen *TransactionGeneratorBigtable) getMutation(colNames ...string) *bigtable.Mutation {
	// Define the allowable time range.
	const minTime int64 = 1451606400 // 2016-01-01
	const maxTime int64 = 1567296000 // 2019-09-01
	const timeRange = maxTime - minTime

	mutation := bigtable.NewMutation()
	unixTime := rand.Int63()%timeRange + minTime
	ts := bigtable.Time(time.Unix(unixTime, 0))
	for _, colName := range colNames {
		mutation.Set(DefaultColumnFamily, colName, ts, gen.randomID())
	}
	return mutation
}

// BigEndian is used here to simulate how an int64 might normally be stored.
func (gen *TransactionGeneratorBigtable) randomID() []byte {
	val := rand.Int63()
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(val))
	return bytes
}
