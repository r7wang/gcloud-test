package datagen

import (
	"context"
	"math/rand"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/r7wang/gcloud-test/timer"
)

// CompanyGeneratorBigtable populates the companies table within the ledger database.
type CompanyGeneratorBigtable struct {
	ctx     context.Context
	client  *bigtable.Client
	metrics *timer.Metrics
}

// NewCompanyGeneratorBigtable returns a new CompanyGeneratorBigtable instance.
func NewCompanyGeneratorBigtable(
	ctx context.Context,
	client *bigtable.Client,
	metrics *timer.Metrics,
) *CompanyGeneratorBigtable {

	return &CompanyGeneratorBigtable{
		ctx:     ctx,
		client:  client,
		metrics: metrics,
	}
}

// Generate adds a predefined list of companies to the table.
//
// Bigtable does not support any form of joins. In order to map an entity with a company ID back to
// the company table or vice versa, this will require multiple lookups across different tables.
// This must be done at the application layer.
func (gen *CompanyGeneratorBigtable) Generate() error {
	defer gen.metrics.Track(time.Now(), "CompanyGenerator.Generate")

	mutations := []*bigtable.Mutation{}
	rowKeys := []string{}
	for _, companyName := range CompanyNames {
		mutation := bigtable.NewMutation()
		mutation.Set(
			DefaultColumnFamily,
			CompanyNameColumn,
			bigtable.Now(),
			[]byte(companyName))
		mutations = append(mutations, mutation)
		rowKeys = append(rowKeys, Int64String(rand.Int63()))
	}
	table := gen.client.Open(CompanyTableName)
	if err := mergeErrors(table.ApplyBulk(gen.ctx, rowKeys, mutations)); err != nil {
		return err
	}
	return nil
}
