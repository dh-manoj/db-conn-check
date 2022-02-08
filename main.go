package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
	_ "github.com/lib/pq"
)

const (
	host   = "localhost"
	port   = 54000
	user   = "supplier-portal"
	dbname = "supplier-portal"
)

type DB struct {
	*dbr.Connection
}

// Supplier represents the suppliers table
type Supplier struct {
	ID                  int64     `json:"id"`
	Name                string    `json:"name" db:"name"`
	CreatedAt           time.Time `json:"created_at" db:"created_at"`
	UpdatedAt           time.Time `json:"updated_at" db:"updated_at"`
	DeletedAt           time.Time `json:"deleted_at" db:"deleted_at"`
	SupplierFinanceID   string    `json:"supplier_finance_id" db:"supplier_finance_id"`
	SupplierCode        *string   `json:"supplier_code" db:"supplier_code"`
	SupplierTaxID       string    `json:"supplier_tax_id" db:"supplier_tax_id"`
	CountryID           int32     `json:"country_id" db:"country_id"`
	CurrencyID          int32     `json:"currency_id" db:"currency_id"`
	CityID              int32     `json:"city_id" db:"city_id"`
	Street              string    `json:"street" db:"street"`
	Complement          string    `json:"complement" db:"complement"`
	Zip                 *int32    `json:"zip"` //@deprecated will be removed soon, https://github.com/deliveryhero/dh-darkstores-supplier-portal/pull/520
	ZipCode             string    `json:"zip_code" db:"zip_code"`
	Email               string    `json:"email" db:"email"`
	Password            string    `json:"password" db:"password"`
	Cellphone           string    `json:"cellphone" db:"cellphone"`
	LandlineTelephone   string    `json:"landline_telephone" db:"landline_telephone"`
	ImageURL            string    `json:"image_url" db:"image_url"`
	StoreCount          int32     `json:"store_count"`
	ProductCount        int32     `json:"product_count"`
	CountryName         string    `json:"country_name"`
	CityName            string    `json:"city_name"`
	TaxOfficeName       *string   `json:"tax_office_name" db:"tax_office_name"`
	RegionName          *string   `json:"region_name" db:"region_name"`
	AddressLine2        *string   `json:"address_line_2" db:"address_line_2"`
	ProjectCode         *string   `json:"project_code" db:"project_code"`
	IBAN                *string   `json:"iban" db:"iban"`
	GroupName           *string   `json:"group_name" db:"group_name"`
	SupplierPayPlanCode *string   `json:"supplier_pay_plan_code" db:"supplier_pay_plan_code"`
}

func TestQueryBeforeCommit(ctx context.Context, conn *DB) error {
	var suppliers []*Supplier

	sess := conn.NewSession(nil)

	fmt.Println("start - open conn:", conn.Stats().OpenConnections)
	tx, _ := sess.Begin()

	defer tx.RollbackUnlessCommitted()

	_, err := tx.Select("*").
		From("suppliers").
		Paginate(10, 50).
		LoadContext(ctx, &suppliers)

	if err != nil {
		fmt.Println("Failed to query supplier")
		return err
	}
	fmt.Println("after transaction query - open conn:", conn.Stats().OpenConnections)

	//blocks here waiting for open connection in case of max connection = 1
	_, err = sess.Select("*").
		From("suppliers").
		Paginate(10, 50).
		LoadContext(ctx, &suppliers)

	if err != nil {
		fmt.Println("Failed to query supplier")
		return err
	}
	fmt.Println("after another query - open conn:", conn.Stats().OpenConnections)

	tx.Commit()

	fmt.Println("TestQueryBeforeCommit end.", conn.Stats().OpenConnections)
	return nil
}

func TestTransacitonWithinTransaction(ctx context.Context, conn *DB) error {
	var suppliers []*Supplier

	sess := conn.NewSession(nil)

	fmt.Println("start - open conn:", conn.Stats().OpenConnections)
	tx, _ := sess.Begin()

	defer tx.RollbackUnlessCommitted()

	_, err := tx.Select("*").
		From("suppliers").
		Paginate(10, 50).
		LoadContext(ctx, &suppliers)

	if err != nil {
		fmt.Println("Failed to query supplier")
		return err
	}
	fmt.Println("after transaction query - open conn:", conn.Stats().OpenConnections)

	//blocks here waiting for open connection in case of max connection = 1
	func() {
		tx1, _ := sess.Begin()

		fmt.Println("new transaction 1 - open conn:", conn.Stats().OpenConnections)

		defer tx1.RollbackUnlessCommitted()

		_, err := tx1.Select("*").
			From("suppliers").
			Paginate(10, 50).
			LoadContext(ctx, &suppliers)

		if err != nil {
			fmt.Println("Failed to query supplier")
			return
		}
		fmt.Println("after transaction 1 query - open conn:", conn.Stats().OpenConnections)
		tx.Commit()
		fmt.Println("after transaction 1 commit - open conn:", conn.Stats().OpenConnections)
	}()
	tx.Commit()

	fmt.Println("TestTransacitonWithinTransaction end.", conn.Stats().OpenConnections)
	return nil
}

func main() {
	password := os.Getenv("DB_PASSWORD")

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully connected!")

	conn := &DB{
		Connection: &dbr.Connection{
			DB:            db,
			Dialect:       dialect.PostgreSQL,
			EventReceiver: &dbr.NullEventReceiver{},
		},
	}

	conn.SetMaxOpenConns(1)
	// conn.SetMaxOpenConns(2)  <----- check with 2 connection the test passes
	conn.SetMaxIdleConns(2)
	conn.SetConnMaxLifetime(1 * time.Second)

	ctx := context.Background()

	// Note both the test gets blocked when max open connection is set to 1
	// enable any one test.
	TestQueryBeforeCommit(ctx, conn)
	//TestTransacitonWithinTransaction(ctx, conn)
}
