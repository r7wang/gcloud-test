package datagen

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/spanner/timer"
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
	defer timer.Track(time.Now(), "UserGenerator.Generate")

	// We are going to probably want enough users to demonstrate scale.
	const numUsers = 200000
	// Batch updates should contain anywhere between 1 MB to 5 MB of data. This should be on the
	// slightly more conservative side.
	const bucketSize = 5000
	const numBuckets = numUsers / bucketSize

	for bucketIdx := 0; bucketIdx < numBuckets; bucketIdx++ {
		min := bucketSize * bucketIdx
		max := min + bucketSize
		if max > numUsers {
			max = numUsers
		}
		err := gen.generateForBucket(min, max)

		if err != nil {
			return err
		}
	}
	return nil
}

func (gen *UserGenerator) generateForBucket(min int, max int) error {
	defer timer.Track(time.Now(), fmt.Sprintf("UserGenerator.generateForBucket-%d", max))

	const tableName = "Users"

	mutations := []*spanner.Mutation{}
	for userIdx := min; userIdx < max; userIdx++ {
		mutation := spanner.InsertMap(tableName, map[string]interface{}{
			"id":           rand.Int63(),
			"name":         fmt.Sprintf("User-%d", userIdx),
			"creationTime": spanner.CommitTimestamp,
		})
		mutations = append(mutations, mutation)
	}
	_, err := gen.client.Apply(gen.ctx, mutations)
	return err
}
