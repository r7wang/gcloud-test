package datagen

const (
	// CompanyTableName names the table that stores company details.
	CompanyTableName = "Companies"
	// UserTableName names the table that stores user details.
	UserTableName = "Users"
	// TransactionTableName names the table that stores transaction details.
	TransactionTableName = "Transactions"
	// TransactionCompanyColumn is the column name for the subject ID of a transaction.
	TransactionCompanyColumn = "CompanyID"
	// TransactionFromUserColumn is the column name for the sender ID of a transaction.
	TransactionFromUserColumn = "FromUserId"
	// TransactionToUserColumn is the column name for the receiver ID of a transaction.
	TransactionToUserColumn = "ToUserId"
	// TransactionBaseID is the lowest value for a monotonically increasing transaction ID.
	TransactionBaseID = 1000000000000000000

	// DefaultColumnFamily is specific to bigtable. This name is intentionally kept short for
	// efficiency.
	DefaultColumnFamily = "cf"
)

// CompanyNames predefines the list of companies to use.
var CompanyNames = []string{
	"Amazon",
	"Apple",
	"Facebook",
	"Google",
	"IBM",
	"Intel",
	"Microsoft",
	"Netflix",
	"Oracle",
	"Visa",
}
