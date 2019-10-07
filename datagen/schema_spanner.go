package datagen

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// SchemaSpanner provides operations for initializing the ledger database, given an active Cloud
// Spanner instance.
type SchemaSpanner struct {
	ctx    context.Context
	client *database.DatabaseAdminClient
	w      io.Writer
}

// NewSchemaSpanner returns a new SchemaSpanner instance.
func NewSchemaSpanner(ctx context.Context, client *database.DatabaseAdminClient) *SchemaSpanner {
	return &SchemaSpanner{ctx: ctx, client: client, w: os.Stdout}
}

// CreateDatabase initializes the ledger database.
//
// Primary keys are randomly generated because there are no combinations of attributes that are
// sufficient for defining uniqueness. Attempting to define uniqueness through any set of
// non-generated keys in the tables below would likely result in undesirable domain constraints.
// These keys will:
//	-	have no data locality
//	-	be very resistant to forming hot spots
//
// When choosing between INT64 and UUIDv4 as a primary key, we note the following differences:
//	-	INT64 consumes 8 bytes; UUIDv4 consumes at least 16 bytes (either BYTE[16] or STRING[36]).
//	-	INT64 has a higher potential for collision than UUIDv4.
//	-	In either case, we may want to consider writing a retry in case of collision for tables
//		that we anticipate to have more records than a certain threshold. This is just a
//		preventative measure to ensure correctness.
func (s *SchemaSpanner) CreateDatabase(db string) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("Invalid database id %s", db)
	}
	op, err := s.client.CreateDatabase(s.ctx, &adminpb.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
		ExtraStatements: schemaKeyFromUserAndTime(),
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(s.ctx); err != nil {
		return err
	}
	return nil
}

func schemaDefault() []string {
	return []string{
		`CREATE TABLE Users(
			Id INT64 NOT NULL,
			Name STRING(2048) NOT NULL,
			CreationTime TIMESTAMP NOT NULL
			OPTIONS(allow_commit_timestamp=true)
		) PRIMARY KEY(Id)`,
		`CREATE TABLE Companies(
			Id INT64 NOT NULL,
			Name STRING(2048) NOT NULL,
			CreationTime TIMESTAMP NOT NULL
			OPTIONS(allow_commit_timestamp=true)
		) PRIMARY KEY(Id)`,
		`CREATE TABLE Transactions(
			Id INT64 NOT NULL,
			CompanyId INT64 NOT NULL,
			FromUserId INT64 NOT NULL,
			ToUserId INT64 NOT NULL,
			Time TIMESTAMP NOT NULL
			OPTIONS(allow_commit_timestamp=true)
		) PRIMARY KEY(Id)`,
	}
}

func schemaKeyFromUserAndTime() []string {
	return []string{
		`CREATE TABLE Users(
			Id INT64 NOT NULL,
			Name STRING(2048) NOT NULL,
			CreationTime TIMESTAMP NOT NULL
			OPTIONS(allow_commit_timestamp=true)
		) PRIMARY KEY(Id)`,
		`CREATE TABLE Companies(
			Id INT64 NOT NULL,
			Name STRING(2048) NOT NULL,
			CreationTime TIMESTAMP NOT NULL
			OPTIONS(allow_commit_timestamp=true)
		) PRIMARY KEY(Id)`,
		`CREATE TABLE Transactions(
			Id INT64 NOT NULL,
			CompanyId INT64 NOT NULL,
			FromUserId INT64 NOT NULL,
			ToUserId INT64 NOT NULL,
			Time TIMESTAMP NOT NULL
			OPTIONS(allow_commit_timestamp=true)
		) PRIMARY KEY(FromUserId, Time)`,
		`CREATE UNIQUE INDEX UniqueId ON Transactions(Id)`,
	}
}
