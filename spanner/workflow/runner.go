package workflow

import (
	"math/rand"
	"time"

	"github.com/r7wang/gcloud-test/spanner/timer"
)

// Runner provides common tools for running tests.
type runner struct {
}

func (r *runner) runTest(testFunc func(r *rand.Rand) error, metricName string) error {
	defer timer.Track(time.Now(), metricName)
	randSeeded := rand.New(rand.NewSource(rand.Int63()))
	for i := 0; i < NumSamples; i++ {
		if err := testFunc(randSeeded); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) runTestReturns(testFunc func(r *rand.Rand) (int64, error), metricName string) ([]int64, error) {
	defer timer.Track(time.Now(), metricName)
	randSeeded := rand.New(rand.NewSource(rand.Int63()))
	keys := []int64{}
	for i := 0; i < NumSamples; i++ {
		key, err := testFunc(randSeeded)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (r *runner) runTestWith(testFunc func(r *rand.Rand, key int64) error, keys []int64, metricName string) error {
	// We may want to assert that keys has a length of NumSamples.
	defer timer.Track(time.Now(), metricName)
	randSeeded := rand.New(rand.NewSource(rand.Int63()))
	for _, key := range keys {
		if err := testFunc(randSeeded, key); err != nil {
			return err
		}
	}
	return nil
}
