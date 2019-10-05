package datagen

const (
	// CompanyTableName names the table that stores company details.
	CompanyTableName = "Companies"
	// CompanyNameColumn is the column name for company names.
	CompanyNameColumn = "Name"
	// UserTableName names the table that stores user details.
	UserTableName = "Users"
	// UserNameColumn is the column name for user names.
	UserNameColumn = "Name"
	// UserCount is the number of users to be generated.
	UserCount = 200000
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
	// TransactionCount is the number of transactions to be generated.
	TransactionCount = 200000000

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
