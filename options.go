package mongods

import "time"

var (
	defaultConfig = config{opTimeout: 5 * time.Second, txnTimeout: 30 * time.Second}
)

type config struct {
	opTimeout  time.Duration
	txnTimeout time.Duration
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
