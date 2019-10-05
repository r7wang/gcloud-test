package datagen

import (
	"encoding/binary"
	"strconv"
)

func int64Bytes(val int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(val))
	return bytes
}

func int64String(val int64) string {
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
