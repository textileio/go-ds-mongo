package mongods

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dsextensions "github.com/textileio/go-datastore-extensions"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	ErrTxnFinalized = errors.New("txn was already finalized")
)

// TODO: context was added to a number of functions - see if you can do it properly
type mongoTxn struct {
	// lock serializes all API access since the
	// mongo session isn't goroutine-safe as mentioned
	// in the docs.
	lock      sync.Mutex
	finalized bool

	m       *MongoDS
	session mongo.Session
}

var _ dsextensions.TxnExt = (*mongoTxn)(nil)

func (m *MongoDS) NewTransaction(_ context.Context, readOnly bool) (datastore.Txn, error) {
	return m.newTransaction(readOnly)
}

func (m *MongoDS) NewTransactionExtended(_ context.Context, readOnly bool) (dsextensions.TxnExt, error) {
	return m.newTransaction(readOnly)
}

func (m *MongoDS) newTransaction(bool) (dsextensions.TxnExt, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}

	session, err := m.m.StartSession()
	if err != nil {
		return nil, fmt.Errorf("starting mongo session: %s", err)
	}

	if err := session.StartTransaction(); err != nil {
		return nil, fmt.Errorf("starting session txn: %s", err)
	}
	return &mongoTxn{
		session: session,
		m:       m,
	}, nil
}

func (t *mongoTxn) Commit(ctx context.Context) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return ErrTxnFinalized
	}

	ctx, cls := context.WithTimeout(ctx, t.m.txnTimeout)
	defer cls()
	if err := t.session.CommitTransaction(ctx); err != nil {
		return fmt.Errorf("commiting session txn: %s", err)
	}
	t.finalized = true
	ctx, cls = context.WithTimeout(ctx, t.m.opTimeout)
	defer cls()
	t.session.EndSession(ctx)

	return nil
}

func (t *mongoTxn) Discard(ctx context.Context) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return
	}

	ctx, cls := context.WithTimeout(context.Background(), t.m.txnTimeout)
	defer cls()
	if err := t.session.AbortTransaction(ctx); err != nil {
		log.Errorf("aborting transaction: %s", err)
	}

	ctx, cls = context.WithTimeout(context.Background(), t.m.opTimeout)
	defer cls()
	t.session.EndSession(ctx)
}

func (t *mongoTxn) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return nil, ErrTxnFinalized
	}
	return t.m.get(ctx, key)
}

func (t *mongoTxn) Has(ctx context.Context, key datastore.Key) (bool, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return false, ErrTxnFinalized
	}
	return t.m.has(ctx, key)
}

func (t *mongoTxn) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return 0, ErrTxnFinalized
	}
	return t.m.getSize(ctx, key)
}

func (t *mongoTxn) Query(ctx context.Context, q query.Query) (query.Results, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return nil, ErrTxnFinalized
	}
	qe := dsextensions.QueryExt{Query: q}
	return t.m.query(ctx, qe)
}

func (t *mongoTxn) QueryExtended(ctx context.Context, q dsextensions.QueryExt) (query.Results, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return nil, ErrTxnFinalized
	}
	return t.m.query(ctx, q)
}

func (t *mongoTxn) Delete(ctx context.Context, key datastore.Key) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return ErrClosed
	}
	return t.m.delete(ctx, key)
}

func (t *mongoTxn) Put(ctx context.Context, key datastore.Key, val []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.finalized {
		return ErrClosed
	}
	return t.m.put(ctx, key, val)
}
