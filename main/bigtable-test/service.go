package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigtable"
	"github.com/r7wang/gcloud-test/timer"
	"github.com/r7wang/gcloud-test/workflow"
)

func createClients(
	ctx context.Context,
	projectName string,
	instanceName string,
) *bigtable.Client {

	client, err := bigtable.NewClient(ctx, projectName, instanceName)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func run(
	ctx context.Context,
	client *bigtable.Client,
	w io.Writer,
) error {

	metrics := timer.NewMetrics()
	oltp := workflow.NewOLTPBigtable(ctx, client, metrics)
	if err := oltp.Run(); err != nil {
		fmt.Fprintf(w, "Failed to run transactional workflow: %v\n", err)
		return err
	}
	fmt.Fprintf(w, metrics.Summarize())
	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: bigtable-test <project_name> <instance_name>`)
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
	client := createClients(ctx, projectName, instanceName)
	defer client.Close()

	if err := run(ctx, client, os.Stdout); err != nil {
		os.Exit(1)
	}
}
