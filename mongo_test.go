package mongods

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	dsextensions "github.com/textileio/go-datastore-extensions"
)

func TestMongoDatastore(t *testing.T) {
	ds := setup(t)
	dstest.SubtestAll(t, ds)
}

func TestQuerySeek(t *testing.T) {
	ds := setup(t)
	type kv struct {
		key   string
		value []byte
	}
	data := []kv{
		{"/1/1", []byte("1.1")},
		{"/1/2", []byte("1.2")},
		{"/1/3", []byte("1.3")},
		{"/2/1", []byte("2.1")},
		{"/2/2", []byte("2.2")},
	}
	for _, d := range data {
		err := ds.Put(datastore.NewKey(d.key), d.value)
		require.NoError(t, err)
	}

	cases := []dsextensions.QueryExt{
		{},                   // All
		{SeekPrefix: "/1/1"}, // All from /1/1
		{SeekPrefix: "/1/3"}, // All from mid /1 key
		{Query: query.Query{Prefix: "/1"}, SeekPrefix: "/1/2"}, // All from /1/2 but only in /1 space.
		{SeekPrefix: "/2/2"}, // Only /2/2
		{SeekPrefix: "/5/1"},
	}
	// Automatically include descending order tests
	for _, c := range cases {
		c.Orders = []query.Order{query.OrderByKeyDescending{}}
		cases = append(cases, c)
	}

	expectedResult := func(q dsextensions.QueryExt) []kv {
		var res []kv

		var prepData []kv
		i, end, inc := 0, len(data), 1
		if len(q.Query.Orders) > 0 {
			i, end, inc = len(data)-1, -1, -1
		}
		for i != end {
			prepData = append(prepData, data[i])
			i += inc
		}
		for _, v := range prepData {
			if !strings.HasPrefix(v.key, q.Query.Prefix) {
				continue
			}
			if q.SeekPrefix != "" {
				if inc == 1 && v.key < q.SeekPrefix {
					continue
				} else if inc == -1 && v.key > q.SeekPrefix {
					continue
				}
			}
			res = append(res, v)
		}
		return res
	}

	for i, q := range cases {
		t.Run(fmt.Sprintf("%d", i+1), func(t *testing.T) {
			result := expectedResult(q)
			res, err := ds.QueryExtended(q)
			require.NoError(t, err)
			all, err := res.Rest()
			require.NoError(t, err)
			require.Len(t, all, len(result))
			for i := range result {
				require.Equal(t, result[i].key, all[i].Key)
			}
		})
	}
}

func TestTxnDiscard(t *testing.T) {
	t.SkipNow() // Should be run with Mongo with replica set, see replset.md
	ds := setupTxn(t)

	txn, err := ds.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	key := datastore.NewKey("/test/thingdiscard")
	if err := txn.Put(key, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	txn.Discard()
	has, err := ds.Has(key)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("key written in aborted transaction still exists")
	}

	ds.Close()
}

func TestTxnCommit(t *testing.T) {
	t.SkipNow() // Should be run with Mongo with replica set, see replset.md
	ds := setupTxn(t)

	txn, err := ds.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	key := datastore.NewKey("/test/thingcommit")
	if err := txn.Put(key, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}
	has, err := ds.Has(key)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("key written in committed transaction does not exist")
	}

	ds.Close()
}

func TestTxnBatch(t *testing.T) {
	t.SkipNow() // Should be run with Mongo with replica set, see replset.md
	ds := setupTxn(t)

	txn, err := ds.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	data := make(map[datastore.Key][]byte)
	for i := 0; i < 10; i++ {
		key := datastore.NewKey(fmt.Sprintf("/test/batch%d", i))
		bytes := make([]byte, 16)
		_, err := rand.Read(bytes)
		if err != nil {
			t.Fatal(err)
		}
		data[key] = bytes

		err = txn.Put(key, bytes)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for key, bytes := range data {
		retrieved, err := ds.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if len(retrieved) != len(bytes) {
			t.Fatal("bytes stored different length from bytes generated")
		}
		for i, b := range retrieved {
			if bytes[i] != b {
				t.Fatal("bytes stored different content from bytes generated")
			}
		}
	}

	ds.Close()
}

func setupTxn(t *testing.T) *MongoDS {
	uri := fmt.Sprintf("mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=my-mongo-set")
	return createMongoDS(t, uri)
}

func setup(t *testing.T) *MongoDS {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, err := pool.Run("mongo", "4.4.0", []string{})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := pool.Purge(resource)
		if err != nil {
			panic(err)
		}
	})

	uri := fmt.Sprintf("mongodb://127.0.0.1:%s", resource.GetPort("27017/tcp"))
	return createMongoDS(t, uri)
}

func createMongoDS(t *testing.T, uri string) *MongoDS {
	ds, err := New(context.Background(), uri, "blocks", "blocky")
	require.NoError(t, err)
	return ds

}
