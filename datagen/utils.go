package datagen

import (
	"encoding/binary"
	"math/rand"
	"strconv"
)

// RandomGeneratedTransactionID returns a randomly generated transaction ID within the valid range
// of randomly generated transactions.
func RandomGeneratedTransactionID(r *rand.Rand) int64 {
	return TransactionBaseID + (r.Int63() % TransactionCount)
}

// RandomGeneratedTransactionIDString returns a randomly generated transaction ID within the valid
// range of randomly generated transactions, as a string.
func RandomGeneratedTransactionIDString(r *rand.Rand) string {
	return Int64String(RandomGeneratedTransactionID(r))
}

// RandomGeneratedTransactionIDRange returns a randomly generated transaction ID range within the
// valid range of randomly generated transactions, as a tuple of strings. The offset must be less
// than the transaction count.
func RandomGeneratedTransactionIDRange(r *rand.Rand, offset int64) (int64, int64) {
	randomID := TransactionBaseID + (r.Int63() % (TransactionCount - offset))
	return randomID, randomID + offset
}

// RandomGeneratedTransactionIDStringRange returns a randomly generated transaction ID range within
// the valid range of randomly generated transactions, as a tuple of strings. The offset must be
// less than the transaction count.
func RandomGeneratedTransactionIDStringRange(r *rand.Rand, offset int64) (string, string) {
	randomID := TransactionBaseID + (r.Int63() % (TransactionCount - offset))
	return Int64String(randomID), Int64String(randomID + offset)
}

func int64Bytes(val int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(val))
	return bytes
}

// Int64String converts an int64 to its string representation.
func Int64String(val int64) string {
	return strconv.FormatInt(val, 10)
}

func mergeErrors(errs []error, err error) error {
	if errs == nil && err == nil {
		return nil
	}
	errsMerged := []error{}
	// The singular error should be prioritized relative to other errors.
	if err != nil {
		errsMerged = append(errsMerged, err)
	}
	if errs != nil {
		errsMerged = append(errsMerged, errs...)
	}

	return &multiError{errs: errsMerged}
}
