package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/spanner"
)

/*
func query(ctx context.Context, w io.Writer, client *spanner.Client) error {
	stmt := spanner.Statement{
		SQL: `SELECT p.PlayerId, p.PlayerName, s.Score, s.Timestamp
		        FROM Players p
		        JOIN Scores s ON p.PlayerId = s.PlayerId
		        ORDER BY s.Score DESC LIMIT 10`}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		var playerID, score int64
		var playerName string
		var timestamp time.Time
		if err := row.Columns(&playerID, &playerName, &score, &timestamp); err != nil {
			return err
		}
		fmt.Fprintf(w, "PlayerId: %d  PlayerName: %s  Score: %s  Timestamp: %s\n",
			playerID, playerName, formatWithCommas(score), timestamp.String()[0:10])
	}
}

func queryWithTimespan(ctx context.Context, w io.Writer, client *spanner.Client, timespan int) error {
	stmt := spanner.Statement{
		SQL: `SELECT p.PlayerId, p.PlayerName, s.Score, s.Timestamp
				FROM Players p
				JOIN Scores s ON p.PlayerId = s.PlayerId
				WHERE s.Timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @Timespan HOUR)
				ORDER BY s.Score DESC LIMIT 10`,
		Params: map[string]interface{}{"Timespan": timespan},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return err
		}
		var playerID, score int64
		var playerName string
		var timestamp time.Time
		if err := row.Columns(&playerID, &playerName, &score, &timestamp); err != nil {
			return err
		}
		fmt.Fprintf(w, "PlayerId: %d  PlayerName: %s  Score: %s  Timestamp: %s\n",
			playerID, playerName, formatWithCommas(score), timestamp.String()[0:10])
	}
}
*/

func run(
	ctx context.Context,
	client *spanner.Client,
	w io.Writer,
	db string,
) error {

	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: spanner-query <database_name>`)
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
