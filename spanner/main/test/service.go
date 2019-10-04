package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/spanner/workflow"
)

func run(
	ctx context.Context,
	client *spanner.Client,
	w io.Writer,
	db string,
) error {

	oltp := workflow.NewOLTP(ctx, client)
	if err := oltp.Run(); err != nil {
		fmt.Fprintf(w, "Failed to run transactional workflow: %v\n", err)
		return err
	}

	olap := workflow.NewOLAP(ctx, client)
	if err := olap.Run(); err != nil {
		fmt.Fprintf(w, "Failed to run analytical workflow: %v\n", err)
		return err
	}

	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: spanner-test <database_name>`)
	}

	flag.Parse()
	flagCount := len(flag.Args())
	if flagCount != 1 {
		flag.Usage()
		os.Exit(2)
	}

	db := flag.Arg(0)
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	if err := run(ctx, client, os.Stdout, db); err != nil {
		os.Exit(1)
	}
}
