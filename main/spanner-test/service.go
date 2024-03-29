package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/spanner"
	"github.com/r7wang/gcloud-test/timer"
	"github.com/r7wang/gcloud-test/workflow"
)

func createClients(ctx context.Context, db string) *spanner.Client {
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	return client
}

func run(
	ctx context.Context,
	client *spanner.Client,
	w io.Writer,
	db string,
) error {

	metrics := timer.NewMetrics()

	oltp := workflow.NewOLTPSpanner(ctx, client, metrics)
	if err := oltp.Run(); err != nil {
		fmt.Fprintf(w, "Failed to run transactional workflow: %v\n", err)
		return err
	}

	olap := workflow.NewOLAPSpanner(ctx, client, metrics)
	if err := olap.Run(); err != nil {
		fmt.Fprintf(w, "Failed to run analytical workflow: %v\n", err)
		return err
	}

	summary, err := metrics.Summarize()
	if err != nil {
		fmt.Fprintf(w, "Failed to summarize metrics: %v\n", err)
	}
	fmt.Fprintf(w, summary)
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
	client := createClients(ctx, db)
	defer client.Close()

	if err := run(ctx, client, os.Stdout, db); err != nil {
		os.Exit(1)
	}
}
