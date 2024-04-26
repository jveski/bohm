package bohm

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaTempl string

// Cluster represents the state shared between multiple instances of this process
// i.e. the database.
type Cluster struct {
	db       *pgxpool.Pool
	inflight sync.WaitGroup

	Logger *slog.Logger
}

func New(db *pgxpool.Pool) *Cluster {
	return &Cluster{
		db:     db,
		Logger: slog.Default(),
	}
}

// NewQueue migrates the schema for the given queue and spawns the corresponding listener if a handler is given.
//
// The schema template is defined in bohm/schema.sql.
func (c *Cluster) NewQueue(ctx context.Context, table string, opts ...Option) error {
	w := &watchQueue{
		cluster:       c,
		runner:        defaultRunner,
		Table:         table,
		Name:          fmt.Sprintf("%s_default", table),
		Notifications: make(chan struct{}, 1),
		LockTTL:       time.Second * 30,
	}
	for _, op := range opts {
		op(w)
	}
	w.Notifications <- struct{}{}

	err := c.db.QueryRow(ctx, `SELECT kcu.column_name
	FROM information_schema.key_column_usage kcu
	WHERE constraint_name = (
		SELECT constraint_name
		FROM information_schema.table_constraints
		WHERE table_name = $1 AND constraint_type = 'PRIMARY KEY'
	);`, table).Scan(&w.PKeyColumn)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("table %q does not exist", table)
	}
	if err != nil {
		return fmt.Errorf("looking up name of primary key column: %w", err)
	}

	err = w.setup(ctx)
	if err != nil {
		return fmt.Errorf("setting up queue: %w", err)
	}

	if w.Handler != nil {
		go w.runner(ctx, w.runWatchLoop(ctx))
		go w.runner(ctx, w.runDispatchLoop(ctx))
	}

	return nil
}

// WaitForHandlers blocks until all currently in-flight workers return.
func (c *Cluster) WaitForHandlers() { c.inflight.Wait() }

type watchQueue struct {
	cluster          *Cluster
	runner           func(context.Context, func() bool)
	Name             string
	Table            string
	ConcurrencyLimit *concurrencyLimiter
	Handler          Handler
	Notifications    chan struct{}
	LockTTL          time.Duration
	KeepalivePercent float64
	PKeyColumn       string
	Filters          []*Filter
}

func (w *watchQueue) setup(ctx context.Context) error {
	templ, err := template.New("").Parse(schemaTempl)
	if err != nil {
		return err
	}

	query := &bytes.Buffer{}
	err = templ.Execute(query, w)
	if err != nil {
		return err
	}

	_, err = w.cluster.db.Exec(ctx, query.String())
	return err
}

func (w *watchQueue) runWatchLoop(ctx context.Context) func() bool {
	return func() bool {
		poolConn, err := w.cluster.db.Acquire(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				w.cluster.Logger.WarnContext(ctx, "error while acquiring db connection", "error", err.Error())
			}
			return false
		}
		conn := poolConn.Hijack()
		defer conn.Close(context.Background())

		_, err = conn.Exec(ctx, "listen bohm_writes_"+w.Name)
		if err != nil {
			w.cluster.Logger.WarnContext(ctx, "error while starting to listen for new queue events", "error", err.Error())
			return false
		}

		for {
			err = w.watchNotifications(ctx, conn)
			if err != nil {
				w.cluster.Logger.ErrorContext(ctx, "error while waiting for queue message notification", "error", err.Error())
				return false
			}

			select {
			case w.Notifications <- struct{}{}:
			default:
			}
		}
	}
}

func (w *watchQueue) watchNotifications(ctx context.Context, conn *pgx.Conn) error {
	var period *time.Duration
	err := conn.QueryRow(ctx, fmt.Sprintf("SELECT * FROM bohm_timetravel_%s()", w.Name)).Scan(&period)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	// Avoid waiting for an unrelated message to dispatch scheduled messages (like retries)
	if period != nil && *period > 0 {
		jitteredPeriod := *period + (time.Duration(rand.Intn(30)) * time.Millisecond)
		w.cluster.Logger.DebugContext(ctx, "setting timeout for next scheduled message visibility", "period", jitteredPeriod)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, jitteredPeriod)
		defer cancel()
	}

	w.cluster.Logger.DebugContext(ctx, "waiting for queue message notification")
	_, err = conn.WaitForNotification(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		w.cluster.Logger.DebugContext(ctx, "queue message watch expired")
		err = nil
	}
	return err
}

func (w *watchQueue) runDispatchLoop(ctx context.Context) func() bool {
	return func() bool {
		for {
			if w.ConcurrencyLimit != nil {
				// Wait for a worker slot to become available
				select {
				case <-ctx.Done():
					return false
				case <-w.ConcurrencyLimit.bucket:
				}
			}

			// Wait for a notification from the db
			select {
			case <-ctx.Done():
				return false
			case <-w.Notifications:
			}

			// Get the message (if any)
			var rowID int64
			var popCount int64
			err := w.cluster.db.QueryRow(ctx, fmt.Sprintf("SELECT * FROM bohm_pop_%s($1);", w.Name), w.LockTTL).Scan(&rowID, &popCount)
			if errors.Is(err, pgx.ErrNoRows) {
				if w.ConcurrencyLimit != nil {
					w.ConcurrencyLimit.bucket <- struct{}{}
				}
				w.cluster.Logger.DebugContext(ctx, "no queue item found")
				return true
			}
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					w.cluster.Logger.ErrorContext(ctx, "unable to get queue message", "error", err.Error())
				}
				if w.ConcurrencyLimit != nil {
					w.ConcurrencyLimit.bucket <- struct{}{}
				}
				return false
			}

			// Run the worker
			w.cluster.inflight.Add(1)
			go func() {
				defer w.cluster.inflight.Add(-1)
				done := w.startKeepaliveLoop(ctx, rowID, popCount)
				defer close(done)

				w.invokeWorker(ctx, rowID, popCount)
				if w.ConcurrencyLimit != nil {
					w.ConcurrencyLimit.bucket <- struct{}{}
				}

				// There may be more items waiting in the queue, most likely the item we just finished if it was
				// concurrently marked for resync. This won't feed back since we continue the loop if no queue item was found.
				select {
				case w.Notifications <- struct{}{}:
				default:
				}
			}()
		}
	}
}

func (w *watchQueue) startKeepaliveLoop(ctx context.Context, rowID, popCount int64) chan<- struct{} {
	done := make(chan struct{})
	interval := time.Duration(float64(w.LockTTL) * w.KeepalivePercent)
	if interval <= 0 {
		return done
	}

	logger := Logger(ctx)
	go runKeepaliveLoop(interval, done, func() bool {
		_, err := w.cluster.db.Exec(ctx, fmt.Sprintf("SELECT bohm_keepalive_%s($1, $2, $3);", w.Name), rowID, popCount, w.LockTTL)
		if err != nil {
			logger.WarnContext(ctx, "error while sending keepalive", "error", err.Error())
			return false
		}
		logger.DebugContext(ctx, "sent keepalive")
		return true
	})

	return done
}

func (w *watchQueue) invokeWorker(ctx context.Context, rowID, popCount int64) {
	start := time.Now()
	logger := w.cluster.Logger.With("rowID", rowID, "popCount", popCount, "queueName", w.Name)
	ctx = context.WithValue(ctx, logKey{}, logger)
	logger.DebugContext(ctx, "processing queue message")

	result, err := w.Handler(ctx, rowID)
	if err != nil {
		result.RequeueAfter = calcRetryBackoff(popCount)
		logger.ErrorContext(ctx, "error while processing message", "error", err.Error())
	}

	if result.RequeueAfter > 0 {
		_, err = w.cluster.db.Exec(ctx, fmt.Sprintf("SELECT bohm_requeue_%s($1, $2);", w.Name), result.RequeueAfter, rowID)
		if err != nil {
			logger.WarnContext(ctx, "error while requeueing item", "requeueAfter", result.RequeueAfter, "error", err.Error())
		} else {
			logger.DebugContext(ctx, "requeue'd item", "requeueAfter", result.RequeueAfter, "latency", time.Since(start))
		}
		return
	}

	_, err = w.cluster.db.Exec(ctx, fmt.Sprintf("SELECT bohm_drop_%s($1);", w.Name), rowID)
	if err != nil {
		logger.WarnContext(ctx, "error while deleting queue message", "error", err.Error())
	} else {
		logger.InfoContext(ctx, "processed queue message", "latency", time.Since(start))
	}
}

func (w *watchQueue) RenderFilters() string {
	if len(w.Filters) == 0 {
		return "true"
	}

	strs := []string{}
	for _, filter := range w.Filters {
		strs = append(strs, fmt.Sprintf("( OLD.%s = NEW.%s )", filter.col, filter.col))
	}
	return strings.Join(strs, " AND ")
}

func calcRetryBackoff(attempt int64) time.Duration {
	const base = time.Millisecond * 10
	dur := base * time.Duration(attempt) * time.Duration(attempt)
	if dur > time.Minute*15 {
		dur = time.Minute * 15
	}
	return dur
}

type concurrencyLimiter struct {
	bucket chan struct{}
}

func newConcurrencyLimiter(max int) *concurrencyLimiter {
	r := &concurrencyLimiter{bucket: make(chan struct{}, max)}
	for i := 0; i < max; i++ {
		r.bucket <- struct{}{}
	}
	return r
}

func defaultRunner(ctx context.Context, fn func() bool) {
	var failures int64
	for {
		ok := fn()
		if ctx.Err() != nil {
			return
		}
		if ok {
			failures = 0
			continue
		}
		failures++

		time.Sleep(calcRetryBackoff(failures))
	}
}

func runKeepaliveLoop(interval time.Duration, done <-chan struct{}, fn func() bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var attempt int64
	for {
		select {
		case <-ticker.C:
		case <-done:
			return
		}

		attempt++
		if !fn() {
			ticker.Reset(calcRetryBackoff(attempt))
			continue
		}
		attempt = 0
		ticker.Reset(interval)
	}
}

type logKey struct{}

// Logger can be used with the contexts provided to Handlers to retrieve a slog
// instance that has values populated by the Bohm internals, or the default
// logger in other contexts.
//
// Essentially this gets you the rowID, popCount, and queueName columns.
func Logger(ctx context.Context) *slog.Logger {
	val := ctx.Value(logKey{})
	if val == nil {
		return slog.Default()
	}
	return val.(*slog.Logger)
}

type Result struct {
	RequeueAfter time.Duration
}
