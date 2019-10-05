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
	ctx    context.Context
	client *bigtable.Client
}

// NewUserGeneratorBigtable returns a new UserGeneratorBigtable instance.
func NewUserGeneratorBigtable(ctx context.Context, client *bigtable.Client) *UserGeneratorBigtable {
	return &UserGeneratorBigtable{ctx: ctx, client: client}
}

// Generate adds a random list of users to the table.
func (gen *UserGeneratorBigtable) Generate() error {
	defer timer.Track(time.Now(), "UserGenerator.Generate")

	const bucketSize = UserCount
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
	defer timer.Track(time.Now(), fmt.Sprintf("UserGenerator.generateForBucket-%d", max))

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
		rowKeys = append(rowKeys, string(rand.Int63()))
	}
	table := gen.client.Open(UserTableName)
	if err := mergeErrors(table.ApplyBulk(gen.ctx, rowKeys, mutations)); err != nil {
		return err
	}
	return nil
}
