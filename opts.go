package bohm

import (
	"context"
	"time"
)

type Option func(*watchQueue)

// WithQueueName sets the queue name. Defaults to ${tablename}_default.
//
// This allows multiple queues to be defined for the same table.
func WithQueueName(name string) Option {
	return func(wq *watchQueue) {
		wq.Name = name
	}
}

// WithConcurrencyLimit limits the concurrency of the queue's handlers within this
// particular process.
func WithConcurrencyLimit(limit int) Option {
	return func(wq *watchQueue) {
		wq.ConcurrencyLimit = newConcurrencyLimiter(limit)
	}
}

// Handler takes the primary key of a row and does something with it.
//
// - Errors are retried
// - Handlers can be invoked concurrently
// - For a given row, only one handler can be active (per queue, across all listeners)
type Handler func(context.Context, int64) (Result, error)

// WithHandler sets the queue's Handler. If not given, the queue schema will still be migrated.
func WithHandler(fn Handler) Option {
	return func(wq *watchQueue) {
		wq.Handler = fn
	}
}

// WithLockTTL determines how long a work item is locked when taken off of the queue.
// When the lock expires another process can start processing the same item if it hasn't already been completed.
//
// Larger values means longer waits when workers crash or are partitioned from the db.
// Smaller values may result in unexpected concurrency for slow Handlers.
// This should be tuned for the particular handler given WithKeepalives.
func WithLockTTL(ttl time.Duration) Option {
	return func(wq *watchQueue) {
		wq.LockTTL = ttl
	}
}

// WithKeepalives sets at what percentage of the lock TTL it should be renewed.
// e.g. 0.8 means the lock TTL will be reset to the configured value when it has 20% of its lifespan remaining.
func WithKeepalives(percent float64) Option {
	return func(wq *watchQueue) {
		wq.KeepalivePercent = percent
	}
}

// WithFilter applies logic to be evaluated by the db when processing changes in order to
// determine if a work item should be enqueued for a given database write.
//
// This is useful for avoiding calling Handlers when there cannot possibly be anything for them to do.
func WithFilter(f *Filter) Option {
	return func(wq *watchQueue) {
		wq.Filters = append(wq.Filters, f)
	}
}

// Filter describes some logic that can be executed by Postgres.
type Filter struct {
	col string
}

// Changed is true when the column has changed during an upgrade.
func Changed(col string) *Filter { return &Filter{col: col} }

// TODO: More conditionals
