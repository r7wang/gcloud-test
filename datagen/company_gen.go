package datagen

import (
	"context"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/timer"
)

// CompanyGenerator populates the companies table within the ledger database.
type CompanyGenerator struct {
	ctx    context.Context
	client *spanner.Client
}

// NewCompanyGenerator returns a new CompanyGenerator instance.
func NewCompanyGenerator(ctx context.Context, client *spanner.Client) *CompanyGenerator {
	return &CompanyGenerator{ctx: ctx, client: client}
}

// Generate adds a predefined list of companies to the table. We can do this in multiple ways.
//	-	Invoke gen.client.Apply() on a set of mutations. This is the least verbose option.
//	-	Invoke gen.client.ReadWriteTransaction() to create the transaction, followed by a
//		txn.BufferWrite() on a set of mutations. This is slightly more verbose but allows the
//		application to add reads that occur within the same transaction.
//	-	Invoke gen.client.ReadWriteTransaction() to create the transaction, followed by a
//		txn.BatchUpdate() on a set of statements. This works for most use cases but has difficulty
//		inserting the commit timestamp into a column with the allow_commit_timestamp option
//		enabled.
//
// See the links below for more information.
//		https://cloud.google.com/spanner/docs/modify-mutation-api
//		https://cloud.google.com/spanner/docs/commit-timestamp
//		https://cloud.google.com/spanner/docs/dml-tasks
//		https://cloud.google.com/spanner/docs/dml-syntax
//		https://cloud.google.com/spanner/docs/transactions
func (gen *CompanyGenerator) Generate() error {
	defer timer.Track(time.Now(), "CompanyGenerator.Generate")

	const tableName = "Companies"
	companyNames := []string{
		"Amazon",
		"Apple",
		"Facebook",
		"Google",
		"IBM",
		"Intel",
		"Microsoft",
		"Netflix",
		"Oracle",
		"Visa",
	}

	mutations := []*spanner.Mutation{}
	for _, companyName := range companyNames {
		mutation := spanner.InsertMap(tableName, map[string]interface{}{
			"id":           rand.Int63(),
			"name":         companyName,
			"creationTime": spanner.CommitTimestamp,
		})
		mutations = append(mutations, mutation)
	}
	_, err := gen.client.Apply(gen.ctx, mutations)
	return err
}
