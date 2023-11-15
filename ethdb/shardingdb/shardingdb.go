package shardingdb

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethereum/go-ethereum/ethdb/leveldb"
)

type Database struct {
	shards []*leveldb.Database
}

func New(path string, cache int, handles int, namespace string, shardNum int) (*Database, error) {
	shards := make([]*leveldb.Database, shardNum)
	for i := 0; i < shardNum; i++ {
		shard := fmt.Sprintf("shard%04d", i)
		shardPath := filepath.Join(path, shard)
		db, err := leveldb.New(shardPath, cache, handles, shard, false)
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

func (db *Database) Shard(index uint64) *leveldb.Database {
	if index >= uint64(len(db.shards)) {
		panic("shard index out of bound")
	}

	return db.shards[index]
}

func (db *Database) ShardNum() uint64 {
	return uint64(len(db.shards))
}

func (db *Database) ShardByHash(h common.Hash) *leveldb.Database {
	index := binary.BigEndian.Uint64(h[len(h)-8:]) % uint64(len(db.shards))
	return db.Shard(index)
}
