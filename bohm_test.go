package bohm

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// func init() {
// 	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
// }

func TestBasics(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	calls := make(chan int64, 1)
	err := cluster.NewQueue(newContext(t), "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		Logger(ctx).InfoContext(ctx, "handling work item")
		calls <- rowID
		return Result{}, nil
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	<-calls
	waitForEmptyQueue(t, db)
}

func TestNoTable(t *testing.T) {
	db := newDB(t)
	cluster := New(db)

	err := cluster.NewQueue(newContext(t), "doesntexist", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		return Result{}, nil
	}))
	require.EqualError(t, err, "table \"doesntexist\" does not exist")
}

func TestNoHandler(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	err := cluster.NewQueue(newContext(t), "test_table")
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 10) // lol
}

func TestRowUniqueness(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	// Just set up the queue triggers without handling any messages
	shortCtx, cancel := context.WithCancel(ctx)
	err := cluster.NewQueue(shortCtx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) { return Result{}, nil }))
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	require.NoError(t, err)
	cancel()

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	_, err = db.Exec(ctx, "UPDATE test_table SET val = 'yo again'")
	require.NoError(t, err)

	// Process the queue messages
	callCount := atomic.Int32{}
	err = cluster.NewQueue(ctx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		callCount.Add(1)
		return Result{}, nil
	}))
	require.NoError(t, err)

	waitForEmptyQueue(t, db)
	assert.Equal(t, int32(2), callCount.Load())
}

func TestErrorRetry(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	calls := make(chan int64)
	err := cluster.NewQueue(ctx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		calls <- rowID
		return Result{}, errors.New("test error")
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	start := time.Now()
	<-calls
	<-calls
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond*10)
	<-calls
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond*40) // exponential backoff
}

func TestRequeueAfter(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	calls := make(chan int64)
	err := cluster.NewQueue(ctx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		calls <- rowID
		return Result{RequeueAfter: time.Millisecond * 5}, nil
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	start := time.Now()
	<-calls
	<-calls
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond*5)
}

func TestRequeueAfterFast(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	calls := make(chan int64)
	err := cluster.NewQueue(ctx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		calls <- rowID
		return Result{RequeueAfter: 1}, nil
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	start := time.Now()
	<-calls
	<-calls
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond*5)
}

func TestArrivalWhileProcessing(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	allCreated := make(chan struct{})
	firstSeen := make(chan struct{})
	closeFirstSeen := sync.OnceFunc(func() { close(firstSeen) })
	err := cluster.NewQueue(ctx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		closeFirstSeen()
		<-allCreated
		return Result{}, nil
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)
	<-firstSeen

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo another world')")
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo yet another world')")
	require.NoError(t, err)

	close(allCreated)
	waitForEmptyQueue(t, db)
}

func TestKeyConcurrency(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	allCreated := make(chan struct{})
	firstSeen := make(chan struct{})
	closeFirstSeen := sync.OnceFunc(func() { close(firstSeen) })
	n := atomic.Int32{}
	err := cluster.NewQueue(ctx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		assert.Equal(t, int32(1), n.Add(1))
		defer n.Add(-1)
		closeFirstSeen()
		<-allCreated
		return Result{}, nil
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)
	<-firstSeen

	_, err = db.Exec(ctx, "UPDATE test_table SET val = 'yo another world'")
	require.NoError(t, err)

	_, err = db.Exec(ctx, "UPDATE test_table SET val = 'yo yet another world'")
	require.NoError(t, err)

	close(allCreated)
	waitForEmptyQueue(t, db)
}

func TestMultipleQueues(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	// The first queue uses the default name and has two active listeners
	callsA := make(chan int64, 1)
	err := cluster.NewQueue(ctx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		callsA <- rowID
		return Result{}, nil
	}))
	require.NoError(t, err)
	err = cluster.NewQueue(ctx, "test_table", WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		callsA <- rowID
		return Result{}, nil
	}))
	require.NoError(t, err)

	// The second queue specifies its own name with a single listener
	callsB := make(chan int64, 1)
	err = cluster.NewQueue(ctx, "test_table", WithQueueName("test_queue"), WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		callsB <- rowID
		return Result{}, nil
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	<-callsA
	<-callsB
	waitForEmptyQueue(t, db)
}

func TestKeepalives(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	calls := make(chan int64, 1)
	err := cluster.NewQueue(newContext(t), "test_table", WithKeepalives(0.8), WithLockTTL(time.Millisecond*10), WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		time.Sleep(time.Millisecond * 30)
		calls <- rowID
		return Result{}, nil
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	<-calls
	waitForEmptyQueue(t, db)
}

func TestKeepaliveBackoff(t *testing.T) {
	calls := make(chan struct{})
	done := make(chan struct{})
	defer close(done)

	go runKeepaliveLoop(time.Millisecond, done, func() bool {
		calls <- struct{}{}
		return false
	})

	start := time.Now()
	<-calls
	<-calls
	assert.GreaterOrEqual(t, time.Since(start), time.Millisecond*10)
}

func TestRateLimiting(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	concurrency := atomic.Int32{}
	err := cluster.NewQueue(newContext(t), "test_table", WithConcurrencyLimit(1), WithHandler(func(ctx context.Context, rowID int64) (Result, error) {
		if concurrency.Add(1) > 1 {
			t.Errorf("expected max concurrency of 1")
		}
		defer concurrency.Add(-1)
		time.Sleep(time.Millisecond)
		return Result{}, nil
	}))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('yo world')")
	require.NoError(t, err)

	waitForEmptyQueue(t, db)
}

func TestFilterChanged(t *testing.T) {
	db := newDB(t)
	cluster := New(db)
	ctx := newContext(t)

	err := cluster.NewQueue(newContext(t), "test_table", WithFilter(Changed("val")))
	require.NoError(t, err)

	_, err = db.Exec(ctx, "INSERT INTO test_table (val) VALUES ('anything')")
	require.NoError(t, err)

	_, err = db.Exec(ctx, "UPDATE test_table SET val = 'anything'")
	require.NoError(t, err)

	var depth int64
	err = db.QueryRow(context.Background(), "SELECT COUNT(*) FROM bohm_queue_test_table_default").Scan(&depth)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth)
}

func newContext(t *testing.T) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

func newDB(t *testing.T) *pgxpool.Pool {
	db, err := pgxpool.New(newContext(t), "postgresql://localhost:5432/postgres")
	require.NoError(t, err)
	t.Cleanup(db.Close)

	t.Cleanup(func() {
		_, err := db.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
		if err != nil {
			panic(err)
		}
	})

	_, err = db.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS test_table (
			idCol SERIAL PRIMARY KEY,
			val TEXT
		);
	`)
	require.NoError(t, err)

	return db
}

func waitForEmptyQueue(t *testing.T, db *pgxpool.Pool) {
	assert.Eventually(t, func() bool {
		var depth int64
		err := db.QueryRow(context.Background(), "SELECT COUNT(*) FROM bohm_queue_test_table_default").Scan(&depth)
		return err == nil && depth == 0
	}, time.Second*2, time.Millisecond*10)
}
