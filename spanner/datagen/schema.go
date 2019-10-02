package datagen

import (
	"context"
	"fmt"
	"io"
	"regexp"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// Schema provides operations for initializing the ledger database, given an active Cloud Spanner
// instance.
type Schema struct {
	ctx    context.Context
	client *database.DatabaseAdminClient
	db     string
}

// NewSchema returns a new Schema object.
func NewSchema(ctx context.Context, client *database.DatabaseAdminClient, db string) *Schema {
	return &Schema{ctx: ctx, client: client, db: db}
}

// CreateDatabase initializes the ledger database.
//
// When choosing between INT64 and UUIDv4 as a primary key, we note the following differences:
//	-	INT64 consumes 8 bytes; UUIDv4 consumes at least 16 bytes (either BYTE[16] or STRING[36]).
//	-	INT64 has a higher potential for collision than UUIDv4.
//	-	In either case, we may want to consider writing a retry in case of collision for tables
//		that we anticipate to have more records than a certain threshold. This is just a
//		preventative measure to ensure correctness.
func (s *Schema) CreateDatabase(w io.Writer) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(s.db)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("Invalid database id %s", s.db)
	}
	op, err := s.client.CreateDatabase(s.ctx, &adminpb.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
		ExtraStatements: []string{
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
				Time TIMESTAMP NOT NULL,
				OPTIONS(allow_commit_timestamp=true)
			) PRIMARY KEY(Id)`,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(s.ctx); err != nil {
		return err
	}
	fmt.Fprintf(w, "Created database [%s]\n", s.db)
	return nil
}
