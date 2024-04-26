package bohm_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jveski/bohm"
)

func ExampleCluster_NewQueue() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, "postgresql://localhost:5432/postgres")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	migrate(db)

	// Set up a handler to process change events
	cluster := bohm.New(db)
	handler := func(ctx context.Context, i int64) (bohm.Result, error) {
		logger := bohm.Logger(ctx) // this logger has some relevant fields already set by Bohm!

		foo, bar := queryRow(db, i)
		logger.Info("current values", "foo", foo, "bar", bar)

		// Calculate the correct value of the 'bar' column
		expectedBar := fmt.Sprintf("%s-but-in-bar", foo)

		if bar != nil && *bar == expectedBar {
			os.Stdout.Write([]byte("in sync\n"))
			return bohm.Result{}, nil // nothing to do!
		}

		writeRow(db, i, expectedBar)
		logger.Info("updated value of bar")

		// - Returning an error causes this work item to be retried with exponential backoff (maybe on another queue listener)
		// - Setting RequeueAfter in the result schedules the next sync of this row
		return bohm.Result{}, nil
	}
	cluster.NewQueue(ctx, "test_table", bohm.WithHandler(handler))

	// Insert a row
	_, err = db.Exec(ctx, "INSERT INTO test_table (string_foo) VALUES ('hello world')")
	if err != nil {
		panic(err)
	}

	// Wait to clean up until things have sync'd
	time.Sleep(time.Millisecond * 500)
	cleanup(db)

	// Output:
	// in sync
}

func migrate(db *pgxpool.Pool) {
	_, err := db.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS test_table (
			pkey SERIAL PRIMARY KEY,
			string_foo TEXT NOT NULL,
			string_bar TEXT
		);
	`)
	if err != nil {
		panic(err)
	}
}

func cleanup(db *pgxpool.Pool) {
	_, err := db.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	if err != nil {
		panic(err)
	}
}

func queryRow(db *pgxpool.Pool, row int64) (string, *string) {
	var foo string
	var bar *string
	err := db.QueryRow(context.Background(), "SELECT string_foo, string_bar FROM test_table WHERE pkey = $1", row).Scan(&foo, &bar)
	if err != nil {
		panic(err)
	}
	return foo, bar
}

func writeRow(db *pgxpool.Pool, row int64, bar any) {
	_, err := db.Exec(context.Background(), "UPDATE test_table SET string_bar = $1 WHERE pkey = $2", bar, row)
	if err != nil {
		panic(err)
	}
}
