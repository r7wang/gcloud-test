package datagen

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