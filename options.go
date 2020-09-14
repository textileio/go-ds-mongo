package mongods

import "time"

var (
	defaultConfig = config{
		opTimeout:  5 * time.Second,
		txnTimeout: 30 * time.Second,
		dbName:     "godsmongo",
		collName:   "kvstore",
	}
)

type config struct {
	opTimeout  time.Duration
	txnTimeout time.Duration
	dbName     string
	collName   string
}

type Option func(*config)

func WithOpTimeout(d time.Duration) Option {
	return func(c *config) {
		c.opTimeout = d
	}
}

func WithTxnTimeout(d time.Duration) Option {
	return func(c *config) {
		c.txnTimeout = d
	}
}

func WithDbName(dbName string) Option {
	return func(c *config) {
		c.dbName = dbName
	}
}

func WithCollName(collName string) Option {
	return func(c *config) {
		c.collName = collName
	}
}
