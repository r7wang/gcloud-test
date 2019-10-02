package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/r7wang/gcloud-test/spanner/datagen"
	"google.golang.org/api/iterator"
)

//	-	We use a randomly generated integer as a key because there is no combination of attributes
//		that defines uniqueness. This also has the side effect of eliminating data locality but
//		also simultaneously decreasing hot spots.
//	-	To avoid the possibility of identifier collisions, the transaction is expected to retry
//		until a unique key is found.
func insertUsers(ctx context.Context, w io.Writer, client *spanner.Client) error {
	// Get number of players to use as an incrementing value for each PlayerName to be inserted
	stmt := spanner.Statement{
		SQL: `SELECT Count(PlayerId) as PlayerCount FROM Players`,
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		return err
	}
	var numberOfPlayers int64 = 0
	if err := row.Columns(&numberOfPlayers); err != nil {
		return err
	}
	// Intialize values for random PlayerId
	rand.Seed(time.Now().UnixNano())
	min := 1000000000
	max := 9000000000
	// Insert 100 player records into the Players table
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmts := []spanner.Statement{}
		for i := 1; i <= 100; i++ {
			numberOfPlayers++
			playerID := rand.Intn(max-min) + min
			playerName := fmt.Sprintf("Player %d", numberOfPlayers)
			stmts = append(stmts, spanner.Statement{
				SQL: `INSERT INTO Players
						(PlayerId, PlayerName)
						VALUES (@playerID, @playerName)`,
				Params: map[string]interface{}{
					"playerID":   playerID,
					"playerName": playerName,
				},
			})
		}
		_, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return err
		}
		return nil
	})
	fmt.Fprintf(w, "Inserted players \n")
	return nil
}

func insertScores(ctx context.Context, w io.Writer, client *spanner.Client) error {
	playerRecordsFound := false
	// Create slice for insert statements
	stmts := []spanner.Statement{}
	// Select all player records
	stmt := spanner.Statement{SQL: `SELECT PlayerId FROM Players`}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	// Insert 4 score records into the Scores table for each player in the Players table
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		playerRecordsFound = true
		var playerID int64
		if err := row.ColumnByName("PlayerId", &playerID); err != nil {
			return err
		}
		// Intialize values for random score and date
		rand.Seed(time.Now().UnixNano())
		min := 1000
		max := 1000000
		for i := 0; i < 4; i++ {
			// Generate random score between 1,000 and 1,000,000
			score := rand.Intn(max-min) + min
			// Generate random day within the past two years
			now := time.Now()
			endDate := now.Unix()
			past := now.AddDate(0, -24, 0)
			startDate := past.Unix()
			randomDateInSeconds := rand.Int63n(endDate-startDate) + startDate
			randomDate := time.Unix(randomDateInSeconds, 0)
			// Add insert statement to stmts slice
			stmts = append(stmts, spanner.Statement{
				SQL: `INSERT INTO Scores
						(PlayerId, Score, Timestamp)
						VALUES (@playerID, @score, @timestamp)`,
				Params: map[string]interface{}{
					"playerID":  playerID,
					"score":     score,
					"timestamp": randomDate,
				},
			})
		}

	}
	if !playerRecordsFound {
		fmt.Fprintln(w, "No player records currently exist. First insert players then insert scores.")
	} else {
		_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			// Commit insert statements for all scores to be inserted as a single transaction
			_, err := txn.BatchUpdate(ctx, stmts)
			return err
		})
		if err != nil {
			return err
		}
		fmt.Fprintln(w, "Inserted scores")
	}
	return nil
}

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

func formatWithCommas(n int64) string {
	numberAsString := strconv.FormatInt(n, 10)
	numberLength := len(numberAsString)
	if numberLength < 4 {
		return numberAsString
	}
	var buffer bytes.Buffer
	comma := []rune(",")
	bufferPosition := numberLength % 3
	if (bufferPosition) > 0 {
		bufferPosition = 3 - bufferPosition
	}
	for i := 0; i < numberLength; i++ {
		if bufferPosition == 3 {
			buffer.WriteRune(comma[0])
			bufferPosition = 0
		}
		bufferPosition++
		buffer.WriteByte(numberAsString[i])
	}
	return buffer.String()
}

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
	dataClient *spanner.Client, w io.Writer,
	db string,
) error {

	schema := datagen.NewSchema(ctx, adminClient, db)
	if err := schema.CreateDatabase(w); err != nil {
		fmt.Fprintf(w, "createDatabase failed with %v", err)
		return err
	}

	if err := insertUsers(ctx, w, dataClient); err != nil {
		fmt.Fprintf(w, "insertUsers failed with %v", err)
		return err
	}

	return nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: datagen <database_name>`)
	}

	flag.Parse()
	flagCount := len(flag.Args())
	if flagCount != 2 {
		flag.Usage()
		os.Exit(2)
	}

	db := flag.Arg(0)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	adminClient, dataClient := createClients(ctx, db)
	if err := run(ctx, adminClient, dataClient, os.Stdout, db); err != nil {
		os.Exit(1)
	}
}
