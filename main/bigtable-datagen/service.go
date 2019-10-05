package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigtable"
	"github.com/r7wang/gcloud-test/datagen"
)

func createClients(
	ctx context.Context,
	projectName string,
	instanceName string,
) (*bigtable.AdminClient, *bigtable.Client) {

	adminClient, err := bigtable.NewAdminClient(ctx, projectName, instanceName)
	if err != nil {
		log.Fatal(err)
	}

	dataClient, err := bigtable.NewClient(ctx, projectName, instanceName)
	if err != nil {
		log.Fatal(err)
	}

	return adminClient, dataClient
}

func run(
	ctx context.Context,
	adminClient *bigtable.AdminClient,
	dataClient *bigtable.Client,
	w io.Writer,
) error {

	schema := datagen.NewSchemaBigtable(ctx, adminClient)
	if err := schema.CreateTables(); err != nil {
		fmt.Fprintf(w, "Failed to instantiate schema: %v\n", err)
		return err
	}
	fmt.Fprintf(w, "Created schema\n")

	transactionGen := datagen.NewTransactionGeneratorBigtable(ctx, dataClient)
	if err := transactionGen.Generate(); err != nil {
		fmt.Fprintf(w, "Failed to generate transactions: %v\n", err)
		return err
	}
	fmt.Fprintf(w, "Inserted transactions\n")

	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: bigtable-datagen <project_name> <instance_name>`)
	}

	flag.Parse()
	flagCount := len(flag.Args())
	if flagCount != 2 {
		flag.Usage()
		os.Exit(2)
	}

	projectName := flag.Arg(0)
	instanceName := flag.Arg(1)
	ctx := context.Background()
	adminClient, dataClient := createClients(ctx, projectName, instanceName)
	defer adminClient.Close()
	defer dataClient.Close()

	if err := run(ctx, adminClient, dataClient, os.Stdout); err != nil {
		os.Exit(1)
	}
}
