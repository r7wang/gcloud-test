package datagen

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/timer"
)

// UserGeneratorSpanner populates the users table within the ledger database.
type UserGeneratorSpanner struct {
	ctx    context.Context
	client *spanner.Client
}

// NewUserGeneratorSpanner returns a new UserGeneratorSpanner instance.
func NewUserGeneratorSpanner(ctx context.Context, client *spanner.Client) *UserGeneratorSpanner {
	return &UserGeneratorSpanner{ctx: ctx, client: client}
}

// Generate adds a random list of users to the table.
//
// See the links below for more information.
//		https://cloud.google.com/spanner/docs/bulk-loading
func (gen *UserGeneratorSpanner) Generate() error {
	defer timer.Track(time.Now(), "UserGenerator.Generate")

	// We are going to probably want enough users to demonstrate scale.
	const bucketSize = 5000
	const numBuckets = UserCount / bucketSize

	for bucketIdx := 0; bucketIdx < numBuckets; bucketIdx++ {
		min := bucketSize * bucketIdx
		max := min + bucketSize
		if max > UserCount {
			max = UserCount
		}
		err := gen.generateForBucket(min, max)
		if err != nil {
			return err
		}
	}
	return nil
}

func (gen *UserGeneratorSpanner) generateForBucket(min int, max int) error {
	defer timer.Track(time.Now(), fmt.Sprintf("UserGenerator.generateForBucket-%d", max))

	mutations := []*spanner.Mutation{}
	for userIdx := min; userIdx < max; userIdx++ {
		mutation := spanner.InsertMap(UserTableName, map[string]interface{}{
			"id":           rand.Int63(),
			"name":         fmt.Sprintf("User-%d", userIdx),
			"creationTime": spanner.CommitTimestamp,
		})
		mutations = append(mutations, mutation)
	}
	_, err := gen.client.Apply(gen.ctx, mutations)
	return err
}
