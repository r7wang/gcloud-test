package datagen

// TransactionGenerator populates the transactions table within the ledger database.
type TransactionGenerator struct {
}

/*
	// Get number of players to use as an incrementing value for each PlayerName to be inserted
	stmt := spanner.Statement{
		SQL: `SELECT Count(PlayerId) as PlayerCount FROM Players`,
	}
	iter := gen.client.Single().Query(gen.ctx, stmt)
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
	_, err = gen.client.ReadWriteTransaction(gen.ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
	fmt.Fprintf(gen.w, "Inserted players \n")
	return nil
*/
