package datagen

import (
	"context"

	"cloud.google.com/go/bigtable"
)

// SchemaBigtable provides operations for initializing the ledger database, given an active Cloud
// Bigtable instance.
type SchemaBigtable struct {
	ctx    context.Context
	client *bigtable.AdminClient
}

// NewSchemaBigtable returns a new SchemaBigtable instance.
func NewSchemaBigtable(ctx context.Context, client *bigtable.AdminClient) *SchemaBigtable {
	return &SchemaBigtable{}
}

// CreateTables initializes the ledger tables.
func (s *SchemaBigtable) CreateTables() error {
	const tableName = "Transactions"
	const familyName = "cf"

	if err := s.client.CreateTable(s.ctx, tableName); err != nil {
		return err
	}
	if err := s.client.CreateColumnFamily(s.ctx, tableName, familyName); err != nil {
		return err
	}
	return nil
}
