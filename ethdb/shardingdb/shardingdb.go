package shardingdb

import (
	"fmt"
	"path/filepath"

	"github.com/ethereum/go-ethereum/ethdb/pebble"

	"github.com/ethereum/go-ethereum/ethdb"

	"github.com/ethereum/go-ethereum/ethdb/leveldb"
)

type Database struct {
	shards []ethdb.KeyValueStore
}

func New(path string, cache int, handles int, namespace string, shardNum int, t string) (*Database, error) {
	shards := make([]ethdb.KeyValueStore, shardNum)
	for i := 0; i < shardNum; i++ {
		shard := fmt.Sprintf("shard%04d", i)
		shardPath := filepath.Join(path, shard)
		existingDb := ethdb.HasPreexistingDb(shardPath)
		var (
			db  ethdb.KeyValueStore
			err error
		)
		if t == ethdb.DBPebble || existingDb == ethdb.DBPebble {
			db, err = pebble.New(shardPath, cache, handles, shard, false)
		} else {
			db, err = leveldb.New(shardPath, cache, handles, shard, false)
		}
		if err != nil {
			return nil, err
		}
		shards[i] = db
	}
	return &Database{
		shards: shards,
	}, nil
}

func (db *Database) Close() error {
	for _, shard := range db.shards {
		shard.Close()
	}
	return nil
}

func (db *Database) Shard(index uint64) ethdb.KeyValueStore {
	if index >= uint64(len(db.shards)) {
		panic("shard index out of bound")
	}

	return db.shards[index]
}

func (db *Database) ShardNum() uint64 {
	return uint64(len(db.shards))
}

func (db *Database) ShardByTriePath(path []byte) ethdb.KeyValueStore {
	return db.Shard(db.ShardIndexByTriePath(path))
}

// ShardIndexByTriePath it shard by prefix of path, to keep order and prefetch quick
// so it max shard is 16
func (db *Database) ShardIndexByTriePath(path []byte) uint64 {
	if len(path) == 0 {
		return 0
	}
	return uint64(path[0]) % uint64(len(db.shards))
}

// TODO comment it to prevent wrong usage
//func (db *Database) ShardByHash(h common.Hash) ethdb.KeyValueStore {
//	return db.Shard(db.ShardIndexByHash(h))
//}
//
//func (db *Database) ShardIndexByHash(h common.Hash) uint64 {
//	return binary.BigEndian.Uint64(h[len(h)-8:]) % uint64(len(db.shards))
//}
