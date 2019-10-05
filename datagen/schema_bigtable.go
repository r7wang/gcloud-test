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
	return &SchemaBigtable{ctx: ctx, client: client}
}

// CreateTables initializes the ledger tables.
func (s *SchemaBigtable) CreateTables() error {
	tableNames := []string{
		CompanyTableName,
		UserTableName,
		TransactionTableName,
	}
	for _, tableName := range tableNames {
		if err := s.client.CreateTable(s.ctx, tableName); err != nil {
			return err
		}
		if err := s.client.CreateColumnFamily(s.ctx, tableName, DefaultColumnFamily); err != nil {
			return err
		}
	}
	return nil
}
