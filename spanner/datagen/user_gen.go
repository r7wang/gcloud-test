package datagen

import (
	"context"
	"fmt"
	"math/rand"

	"cloud.google.com/go/spanner"
)

// UserGenerator populates the users table within the ledger database.
type UserGenerator struct {
	ctx    context.Context
	client *spanner.Client
}

// NewUserGenerator returns a new UserGenerator instance.
func NewUserGenerator(ctx context.Context, client *spanner.Client) *UserGenerator {
	return &UserGenerator{ctx: ctx, client: client}
}

// Generate adds a random list of users to the table.
//
// See the links below for more information.
//		https://cloud.google.com/spanner/docs/bulk-loading
func (gen *UserGenerator) Generate() error {
	const tableName = "Users"
	// We are going to probably want enough users to demonstrate scale.
	const numUsers = 200000
	// Batch updates should contain anywhere between 1 MB to 5 MB of data. This should be on the
	// slightly more conservative side.
	const bucketSize = 50000

	mutations := []*spanner.Mutation{}
	for i := 1; i <= numUsers/bucketSize; i++ {
		mutation := spanner.InsertMap(tableName, map[string]interface{}{
			"id":           rand.Uint64(),
			"name":         fmt.Sprintf("User-%d", i),
			"creationTime": spanner.CommitTimestamp,
		})
		mutations = append(mutations, mutation)
	}
	_, err := gen.client.Apply(gen.ctx, mutations)
	return err
}
