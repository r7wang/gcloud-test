package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/r7wang/gcloud-test/spanner/datagen"
)

func createClients(ctx context.Context, db string) (*database.DatabaseAdminClient, *spanner.Client) {
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	return adminClient, dataClient
}

func run(
	ctx context.Context,
	adminClient *database.DatabaseAdminClient,
	dataClient *spanner.Client,
	w io.Writer,
	db string,
) error {

	schema := datagen.NewSchema(ctx, adminClient)
	if err := schema.CreateDatabase(db); err != nil {
		fmt.Fprintf(w, "Failed to instantiate schema: %v\n", err)
		return err
	}
	fmt.Fprintf(w, "Created database [%s]\n", db)

	companyGen := datagen.NewCompanyGenerator(ctx, dataClient)
	if err := companyGen.Generate(); err != nil {
		fmt.Fprintf(w, "Failed to generate companies: %v\n", err)
		return err
	}
	fmt.Fprintf(w, "Inserted companies\n")

	userGen := datagen.NewUserGenerator(ctx, dataClient)
	if err := userGen.Generate(); err != nil {
		fmt.Fprintf(w, "Failed to generate users: %v\n", err)
		return err
	}
	fmt.Fprintf(w, "Inserted users\n")

	transactionGen := datagen.NewTransactionGenerator(ctx, dataClient)
	if err := transactionGen.Generate(); err != nil {
		fmt.Fprintf(w, "Failed to generate transactions: %v\n", err)
		return err
	}
	fmt.Fprintf(w, "Inserted transactions\n")

	return nil
}

// The goal of this project is to take a given instance, and enable it to serve a variety of query
// tests. As part of this process, there are metrics that can be collected on performance of bulk
// inserts.
func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: spanner-datagen <database_name>`)
	}

	flag.Parse()
	flagCount := len(flag.Args())
	if flagCount != 1 {
		flag.Usage()
		os.Exit(2)
	}

	db := flag.Arg(0)
	ctx := context.Background()
	adminClient, dataClient := createClients(ctx, db)
	if err := run(ctx, adminClient, dataClient, os.Stdout, db); err != nil {
		os.Exit(1)
	}
}
