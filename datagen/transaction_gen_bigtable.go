package datagen

// TransactionGeneratorBigtable populates the transactions table within the ledger instance.
type TransactionGeneratorBigtable struct {
}

// NewTransactionGeneratorBigtable returns a new TransactionGeneratorBigtable instance.
func NewTransactionGeneratorBigtable() *TransactionGeneratorBigtable {
	return &TransactionGeneratorBigtable{}
}

// Generate adds a random list of transactions to the table.
func (gen *TransactionGeneratorBigtable) Generate() error {
	return nil
}
