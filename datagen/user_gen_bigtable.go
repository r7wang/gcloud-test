package datagen

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/r7wang/gcloud-test/timer"
)

// UserGeneratorBigtable populates the users table within the ledger database.
type UserGeneratorBigtable struct {
	ctx     context.Context
	client  *bigtable.Client
	metrics *timer.Metrics
}

// NewUserGeneratorBigtable returns a new UserGeneratorBigtable instance.
func NewUserGeneratorBigtable(
	ctx context.Context,
	client *bigtable.Client,
	metrics *timer.Metrics,
) *UserGeneratorBigtable {

	return &UserGeneratorBigtable{
		ctx:     ctx,
		client:  client,
		metrics: metrics,
	}
}

// Generate adds a random list of users to the table.
//
// According to the documentation, there is a hard limit of 100K mutations per bulk application,
// however in testing, we've found that more than 100K mutations will still work. Even then, this
// limit may not result in optimal performance.
//
// See the links below for more information:
//		https://godoc.org/cloud.google.com/go/bigtable#Table.ApplyBulk
func (gen *UserGeneratorBigtable) Generate() error {
	defer gen.metrics.Track(time.Now(), "UserGenerator.Generate")

	const bucketSize = 100000
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

func (gen *UserGeneratorBigtable) generateForBucket(min int, max int) error {
	defer gen.metrics.Track(time.Now(), "UserGenerator.generateForBucket")

	mutations := []*bigtable.Mutation{}
	rowKeys := []string{}
	for userIdx := min; userIdx < max; userIdx++ {
		mutation := bigtable.NewMutation()
		mutation.Set(
			DefaultColumnFamily,
			UserNameColumn,
			bigtable.Now(),
			[]byte(fmt.Sprintf("User-%d", userIdx)))
		mutations = append(mutations, mutation)
		rowKeys = append(rowKeys, Int64String(rand.Int63()))
	}
	table := gen.client.Open(UserTableName)
	if err := mergeErrors(table.ApplyBulk(gen.ctx, rowKeys, mutations)); err != nil {
		return err
	}
	return nil
}
