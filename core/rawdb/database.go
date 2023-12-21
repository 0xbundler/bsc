// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/ethdb/shardingdb"

	"github.com/olekukonko/tablewriter"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/log"
)

// freezerdb is a database wrapper that enabled freezer data retrievals.
type freezerdb struct {
	ancientRoot string
	ethdb.KeyValueStore
	ethdb.AncientStore
	diffStore ethdb.KeyValueStore
	sharding  *shardingdb.Database
}

func (frdb *freezerdb) Sharded() bool {
	return true
}

func (frdb *freezerdb) ShardNum() uint64 {
	if frdb.sharding == nil {
		return 0
	}
	return frdb.sharding.ShardNum()
}

func (frdb *freezerdb) Shard(index uint64) ethdb.KeyValueStore {
	if frdb.sharding == nil {
		return frdb
	}
	return frdb.sharding.Shard(index)
}

func (frdb *freezerdb) ShardByHash(h common.Hash) ethdb.KeyValueStore {
	if frdb.sharding == nil {
		return frdb
	}
	return frdb.sharding.ShardByHash(h)
}

// AncientDatadir returns the path of root ancient directory.
func (frdb *freezerdb) AncientDatadir() (string, error) {
	return frdb.ancientRoot, nil
}

// Close implements io.Closer, closing both the fast key-value store as well as
// the slow ancient tables.
func (frdb *freezerdb) Close() error {
	var errs []error
	if err := frdb.AncientStore.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := frdb.KeyValueStore.Close(); err != nil {
		errs = append(errs, err)
	}
	if frdb.diffStore != nil {
		if err := frdb.diffStore.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if frdb.sharding != nil {
		if err := frdb.sharding.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

func (frdb *freezerdb) DiffStore() ethdb.KeyValueStore {
	return frdb.diffStore
}

func (frdb *freezerdb) SetDiffStore(diff ethdb.KeyValueStore) {
	if frdb.diffStore != nil {
		frdb.diffStore.Close()
	}
	frdb.diffStore = diff
}

// Freeze is a helper method used for external testing to trigger and block until
// a freeze cycle completes, without having to sleep for a minute to trigger the
// automatic background run.
func (frdb *freezerdb) Freeze(threshold uint64) error {
	if frdb.AncientStore.(*chainFreezer).readonly {
		return errReadOnly
	}
	// Set the freezer threshold to a temporary value
	defer func(old uint64) {
		frdb.AncientStore.(*chainFreezer).threshold.Store(old)
	}(frdb.AncientStore.(*chainFreezer).threshold.Load())
	frdb.AncientStore.(*chainFreezer).threshold.Store(threshold)

	// Trigger a freeze cycle and block until it's done
	trigger := make(chan struct{}, 1)
	frdb.AncientStore.(*chainFreezer).trigger <- trigger
	<-trigger
	return nil
}

// nofreezedb is a database wrapper that disables freezer data retrievals.
type nofreezedb struct {
	ethdb.KeyValueStore
	diffStore ethdb.KeyValueStore
	sharding  *shardingdb.Database
}

func (db *nofreezedb) Sharded() bool {
	return true
}

func (db *nofreezedb) ShardNum() uint64 {
	if db.sharding == nil {
		return 0
	}
	return db.sharding.ShardNum()
}

func (db *nofreezedb) Shard(index uint64) ethdb.KeyValueStore {
	if db.sharding == nil {
		return db
	}
	return db.sharding.Shard(index)
}

func (db *nofreezedb) ShardByHash(h common.Hash) ethdb.KeyValueStore {
	if db.sharding == nil {
		return db
	}
	return db.sharding.ShardByHash(h)
}

// HasAncient returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) HasAncient(kind string, number uint64) (bool, error) {
	return false, errNotSupported
}

// Ancient returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Ancient(kind string, number uint64) ([]byte, error) {
	return nil, errNotSupported
}

// AncientRange returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) AncientRange(kind string, start, max, maxByteSize uint64) ([][]byte, error) {
	return nil, errNotSupported
}

// Ancients returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// Ancients returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) ItemAmountInAncient() (uint64, error) {
	return 0, errNotSupported
}

// Tail returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Tail() (uint64, error) {
	return 0, errNotSupported
}

// AncientSize returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) AncientSize(kind string) (uint64, error) {
	return 0, errNotSupported
}

// ModifyAncients is not supported.
func (db *nofreezedb) ModifyAncients(func(ethdb.AncientWriteOp) error) (int64, error) {
	return 0, errNotSupported
}

// TruncateHead returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) TruncateHead(items uint64) (uint64, error) {
	return 0, errNotSupported
}

// TruncateTail returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) TruncateTail(items uint64) (uint64, error) {
	return 0, errNotSupported
}

// Sync returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) Sync() error {
	return errNotSupported
}

func (db *nofreezedb) DiffStore() ethdb.KeyValueStore {
	return db.diffStore
}

// Close implements io.Closer, closing both the fast key-value store as well as
// the slow ancient tables.
func (frdb *nofreezedb) Close() error {
	var errs []error
	if err := frdb.KeyValueStore.Close(); err != nil {
		errs = append(errs, err)
	}
	if frdb.diffStore != nil {
		if err := frdb.diffStore.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if frdb.sharding != nil {
		if err := frdb.sharding.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

func (db *nofreezedb) SetDiffStore(diff ethdb.KeyValueStore) {
	db.diffStore = diff
}

func (db *nofreezedb) ReadAncients(fn func(reader ethdb.AncientReaderOp) error) (err error) {
	// Unlike other ancient-related methods, this method does not return
	// errNotSupported when invoked.
	// The reason for this is that the caller might want to do several things:
	// 1. Check if something is in freezer,
	// 2. If not, check leveldb.
	//
	// This will work, since the ancient-checks inside 'fn' will return errors,
	// and the leveldb work will continue.
	//
	// If we instead were to return errNotSupported here, then the caller would
	// have to explicitly check for that, having an extra clause to do the
	// non-ancient operations.
	return fn(db)
}

func (db *nofreezedb) AncientOffSet() uint64 {
	return 0
}

// MigrateTable processes the entries in a given table in sequence
// converting them to a new format if they're of an old format.
func (db *nofreezedb) MigrateTable(kind string, convert convertLegacyFn) error {
	return errNotSupported
}

// AncientDatadir returns an error as we don't have a backing chain freezer.
func (db *nofreezedb) AncientDatadir() (string, error) {
	return "", errNotSupported
}

// NewDatabase creates a high level database on top of a given key-value data
// store without a freezer moving immutable chain segments into cold storage.
func NewDatabase(db ethdb.KeyValueStore) ethdb.Database {
	return &nofreezedb{KeyValueStore: db}
}

// NewFreezerDb only create a freezer without statedb.
func NewFreezerDb(db ethdb.KeyValueStore, frz, namespace string, readonly bool, newOffSet uint64) (*Freezer, error) {
	// Create the idle freezer instance, this operation should be atomic to avoid mismatch between offset and acientDB.
	frdb, err := NewFreezer(frz, namespace, readonly, newOffSet, freezerTableSize, chainFreezerNoSnappy)
	if err != nil {
		return nil, err
	}
	return frdb, nil
}

// resolveChainFreezerDir is a helper function which resolves the absolute path
// of chain freezer by considering backward compatibility.
func resolveChainFreezerDir(ancient string) string {
	// Check if the chain freezer is already present in the specified
	// sub folder, if not then two possibilities:
	// - chain freezer is not initialized
	// - chain freezer exists in legacy location (root ancient folder)
	freezer := path.Join(ancient, chainFreezerName)
	if !common.FileExist(freezer) {
		if !common.FileExist(ancient) {
			// The entire ancient store is not initialized, still use the sub
			// folder for initialization.
		} else {
			// Ancient root is already initialized, then we hold the assumption
			// that chain freezer is also initialized and located in root folder.
			// In this case fallback to legacy location.
			freezer = ancient
			log.Info("Found legacy ancient chain path", "location", ancient)
		}
	}
	return freezer
}

// NewDatabaseWithFreezer creates a high level database on top of a given key-
// value data store with a freezer moving immutable chain segments into cold
// storage. The passed ancient indicates the path of root ancient directory
// where the chain freezer can be opened.
func NewDatabaseWithFreezer(db ethdb.KeyValueStore, sharding *shardingdb.Database, ancient, namespace string, readonly, disableFreeze, isLastOffset, pruneAncientData bool) (ethdb.Database, error) {
	var offset uint64
	// The offset of ancientDB should be handled differently in different scenarios.
	if isLastOffset {
		offset = ReadOffSetOfLastAncientFreezer(db)
	} else {
		offset = ReadOffSetOfCurrentAncientFreezer(db)
	}

	if pruneAncientData && !disableFreeze && !readonly {
		frdb, err := newPrunedFreezer(resolveChainFreezerDir(ancient), db, offset)
		if err != nil {
			return nil, err
		}

		go frdb.freeze()
		if !readonly {
			WriteAncientType(db, PruneFreezerType)
		}
		return &freezerdb{
			KeyValueStore: db,
			AncientStore:  frdb,
			sharding:      sharding,
		}, nil
	}

	if pruneAncientData {
		log.Error("pruneancient not take effect, disableFreezer or readonly be set")
	}

	if prunedFrozen := ReadFrozenOfAncientFreezer(db); prunedFrozen > offset {
		offset = prunedFrozen
	}

	// Create the idle freezer instance
	frdb, err := newChainFreezer(resolveChainFreezerDir(ancient), namespace, readonly, offset)
	if err != nil {
		printChainMetadata(db)
		return nil, err
	}

	// Since the freezer can be stored separately from the user's key-value database,
	// there's a fairly high probability that the user requests invalid combinations
	// of the freezer and database. Ensure that we don't shoot ourselves in the foot
	// by serving up conflicting data, leading to both datastores getting corrupted.
	//
	//   - If both the freezer and key-value store is empty (no genesis), we just
	//     initialized a new empty freezer, so everything's fine.
	//   - If the key-value store is empty, but the freezer is not, we need to make
	//     sure the user's genesis matches the freezer. That will be checked in the
	//     blockchain, since we don't have the genesis block here (nor should we at
	//     this point care, the key-value/freezer combo is valid).
	//   - If neither the key-value store nor the freezer is empty, cross validate
	//     the genesis hashes to make sure they are compatible. If they are, also
	//     ensure that there's no gap between the freezer and subsequently leveldb.
	//   - If the key-value store is not empty, but the freezer is we might just be
	//     upgrading to the freezer release, or we might have had a small chain and
	//     not frozen anything yet. Ensure that no blocks are missing yet from the
	//     key-value store, since that would mean we already had an old freezer.

	// If the genesis hash is empty, we have a new key-value store, so nothing to
	// validate in this method. If, however, the genesis hash is not nil, compare
	// it to the freezer content.
	// Only to check the followings when offset equal to 0, otherwise the block number
	// in ancientdb did not start with 0, no genesis block in ancientdb as well.

	if kvgenesis, _ := db.Get(headerHashKey(0)); offset == 0 && len(kvgenesis) > 0 {
		if frozen, _ := frdb.Ancients(); frozen > 0 {
			// If the freezer already contains something, ensure that the genesis blocks
			// match, otherwise we might mix up freezers across chains and destroy both
			// the freezer and the key-value store.
			frgenesis, err := frdb.Ancient(ChainFreezerHashTable, 0)
			if err != nil {
				printChainMetadata(db)
				return nil, fmt.Errorf("failed to retrieve genesis from ancient %v", err)
			} else if !bytes.Equal(kvgenesis, frgenesis) {
				printChainMetadata(db)
				return nil, fmt.Errorf("genesis mismatch: %#x (leveldb) != %#x (ancients)", kvgenesis, frgenesis)
			}
			// Key-value store and freezer belong to the same network. Ensure that they
			// are contiguous, otherwise we might end up with a non-functional freezer.
			if kvhash, _ := db.Get(headerHashKey(frozen)); len(kvhash) == 0 {
				// Subsequent header after the freezer limit is missing from the database.
				// Reject startup if the database has a more recent head.
				if head := *ReadHeaderNumber(db, ReadHeadHeaderHash(db)); head > frozen-1 {
					// Find the smallest block stored in the key-value store
					// in range of [frozen, head]
					var number uint64
					for number = frozen; number <= head; number++ {
						if present, _ := db.Has(headerHashKey(number)); present {
							break
						}
					}
					// We are about to exit on error. Print database metdata beore exiting
					printChainMetadata(db)
					return nil, fmt.Errorf("gap in the chain between ancients [0 - #%d] and leveldb [#%d - #%d] ",
						frozen-1, number, head)
				}
				// Database contains only older data than the freezer, this happens if the
				// state was wiped and reinited from an existing freezer.
			}
			// Otherwise, key-value store continues where the freezer left off, all is fine.
			// We might have duplicate blocks (crash after freezer write but before key-value
			// store deletion, but that's fine).
		} else {
			// If the freezer is empty, ensure nothing was moved yet from the key-value
			// store, otherwise we'll end up missing data. We check block #1 to decide
			// if we froze anything previously or not, but do take care of databases with
			// only the genesis block.
			if ReadHeadHeaderHash(db) != common.BytesToHash(kvgenesis) {
				// Key-value store contains more data than the genesis block, make sure we
				// didn't freeze anything yet.
				if kvblob, _ := db.Get(headerHashKey(1)); len(kvblob) == 0 {
					printChainMetadata(db)
					return nil, errors.New("ancient chain segments already extracted, please set --datadir.ancient to the correct path")
				}
				// Block #1 is still in the database, we're allowed to init a new freezer
			}
			// Otherwise, the head header is still the genesis, we're allowed to init a new
			// freezer.
		}
	}
	// no prune ancinet start success
	if !readonly {
		WriteAncientType(db, EntireFreezerType)
	}
	// Freezer is consistent with the key-value database, permit combining the two
	if !disableFreeze && !frdb.readonly {
		frdb.wg.Add(1)
		go func() {
			frdb.freeze(db)
			frdb.wg.Done()
		}()
	}
	return &freezerdb{
		ancientRoot:   ancient,
		KeyValueStore: db,
		AncientStore:  frdb,
		sharding:      sharding,
	}, nil
}

// NewMemoryDatabase creates an ephemeral in-memory key-value database without a
// freezer moving immutable chain segments into cold storage.
func NewMemoryDatabase() ethdb.Database {
	return NewDatabase(memorydb.New())
}

// NewMemoryDatabaseWithCap creates an ephemeral in-memory key-value database
// with an initial starting capacity, but without a freezer moving immutable
// chain segments into cold storage.
func NewMemoryDatabaseWithCap(size int) ethdb.Database {
	return NewDatabase(memorydb.NewWithCap(size))
}

// NewLevelDBDatabase creates a persistent key-value database without a freezer
// moving immutable chain segments into cold storage.
func NewLevelDBDatabase(file string, cache int, handles int, namespace string, readonly bool) (ethdb.Database, error) {
	db, err := leveldb.New(file, cache, handles, namespace, readonly)
	if err != nil {
		return nil, err
	}
	log.Info("Using LevelDB as the backing database")
	return NewDatabase(db), nil
}

// NewLevelDBDatabaseWithFreezer creates a persistent key-value database with a
// freezer moving immutable chain segments into cold storage. The passed ancient
// indicates the path of root ancient directory where the chain freezer can be
// opened.
// just used for test now
func NewLevelDBDatabaseWithFreezer(file string, cache int, handles int, ancient string, namespace string, readonly, disableFreeze, isLastOffset, pruneAncientData bool) (ethdb.Database, error) {
	handles = handles / (shardNum + 1)
	kvdb, err := leveldb.New(file, cache, handles, namespace, readonly)
	if err != nil {
		return nil, err
	}
	log.Info("init sharding database", "shard", shardNum)
	sharding, err := shardingdb.New(file, cache, handles, namespace, shardNum, "")
	if err != nil {
		return nil, err
	}
	frdb, err := NewDatabaseWithFreezer(kvdb, sharding, ancient, namespace, readonly, disableFreeze, isLastOffset, pruneAncientData)
	if err != nil {
		kvdb.Close()
		return nil, err
	}
	return frdb, nil
}

// NewPebbleDBDatabase creates a persistent key-value database without a freezer
// moving immutable chain segments into cold storage.
func NewPebbleDBDatabase(file string, cache int, handles int, namespace string, readonly bool) (ethdb.Database, error) {
	db, err := pebble.New(file, cache, handles, namespace, readonly)
	if err != nil {
		return nil, err
	}
	return NewDatabase(db), nil
}

// OpenOptions contains the options to apply when opening a database.
// OBS: If AncientsDirectory is empty, it indicates that no freezer is to be used.
type OpenOptions struct {
	Type              string // "leveldb" | "pebble"
	Directory         string // the datadir
	AncientsDirectory string // the ancients-dir
	Namespace         string // the namespace for database relevant metrics
	Cache             int    // the capacity(in megabytes) of the data caching
	Handles           int    // number of files to be open simultaneously
	ReadOnly          bool

	DisableFreeze    bool
	IsLastOffset     bool
	PruneAncientData bool
}

// openKeyValueDatabase opens a disk-based key-value database, e.g. leveldb or pebble.
//
//	                      type == null          type != null
//	                   +----------------------------------------
//	db is non-existent |  leveldb default  |  specified type
//	db is existent     |  from db         |  specified type (if compatible)
func openKeyValueDatabase(o OpenOptions) (ethdb.Database, error) {
	// Reject any unsupported database type
	if len(o.Type) != 0 && o.Type != ethdb.DBLeveldb && o.Type != ethdb.DBPebble {
		return nil, fmt.Errorf("unknown db.engine %v", o.Type)
	}
	// Retrieve any pre-existing database's type and use that or the requested one
	// as long as there's no conflict between the two types
	existingDb := ethdb.HasPreexistingDb(o.Directory)
	if len(existingDb) != 0 && len(o.Type) != 0 && o.Type != existingDb {
		return nil, fmt.Errorf("db.engine choice was %v but found pre-existing %v database in specified data directory", o.Type, existingDb)
	}
	if o.Type == ethdb.DBPebble || existingDb == ethdb.DBPebble {
		log.Info("Using pebble as the backing database")
		return NewPebbleDBDatabase(o.Directory, o.Cache, o.Handles, o.Namespace, o.ReadOnly)
	}
	if o.Type == ethdb.DBLeveldb || existingDb == ethdb.DBLeveldb {
		log.Info("Using leveldb as the backing database")
		return NewLevelDBDatabase(o.Directory, o.Cache, o.Handles, o.Namespace, o.ReadOnly)
	}
	// No pre-existing database, no user-requested one either. Default to Pebble
	// on supported platforms and LevelDB on anything else.
	// 	log.Info("Defaulting to pebble as the backing database")
	// 	return NewPebbleDBDatabase(o.Directory, o.Cache, o.Handles, o.Namespace, o.ReadOnly)
	log.Info("Defaulting to leveldb as the backing database")
	return NewLevelDBDatabase(o.Directory, o.Cache, o.Handles, o.Namespace, o.ReadOnly)
}

// Open opens both a disk-based key-value database such as leveldb or pebble, but also
// integrates it with a freezer database -- if the AncientDir option has been
// set on the provided OpenOptions.
// The passed o.AncientDir indicates the path of root ancient directory where
// the chain freezer can be opened.
const shardNum = 2

func Open(o OpenOptions) (ethdb.Database, error) {
	no := o
	if !no.DisableFreeze {
		no.Handles = no.Handles / (shardNum + 1)
		no.Cache = no.Cache / (shardNum + 1)
	}
	kvdb, err := openKeyValueDatabase(no)
	if err != nil {
		return nil, err
	}
	if len(no.AncientsDirectory) == 0 {
		return kvdb, nil
	}
	var sharding *shardingdb.Database
	if !no.DisableFreeze {
		log.Info("init sharding database in Open", "shard", shardNum)
		sharding, err = shardingdb.New(no.Directory, no.Cache, no.Handles, no.Namespace, shardNum, no.Type)
		if err != nil {
			return nil, err
		}
	}
	frdb, err := NewDatabaseWithFreezer(kvdb, sharding, no.AncientsDirectory, no.Namespace, no.ReadOnly, no.DisableFreeze, no.IsLastOffset, no.PruneAncientData)
	if err != nil {
		kvdb.Close()
		return nil, err
	}
	return frdb, nil
}

type counter uint64

func (c counter) String() string {
	return fmt.Sprintf("%d", c)
}

func (c counter) Percentage(current uint64) string {
	return fmt.Sprintf("%d", current*100/uint64(c))
}

// stat stores sizes and count for a parameter
type stat struct {
	size  common.StorageSize
	count counter
}

// Add size to the stat and increase the counter by 1
func (s *stat) Add(size common.StorageSize) {
	s.size += size
	s.count++
}

func (s *stat) Size() string {
	return s.size.String()
}

func (s *stat) Count() string {
	return s.count.String()
}
func AncientInspect(db ethdb.Database) error {
	offset := counter(ReadOffSetOfCurrentAncientFreezer(db))
	// Get number of ancient rows inside the freezer.
	ancients := counter(0)
	if count, err := db.ItemAmountInAncient(); err != nil {
		log.Error("failed to get the items amount in ancientDB", "err", err)
		return err
	} else {
		ancients = counter(count)
	}
	var endNumber counter
	if offset+ancients <= 0 {
		endNumber = 0
	} else {
		endNumber = offset + ancients - 1
	}
	stats := [][]string{
		{"Offset/StartBlockNumber", "Offset/StartBlockNumber of ancientDB", offset.String()},
		{"Amount of remained items in AncientStore", "Remaining items of ancientDB", ancients.String()},
		{"The last BlockNumber within ancientDB", "The last BlockNumber", endNumber.String()},
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Database", "Category", "Items"})
	table.SetFooter([]string{"", "AncientStore information", ""})
	table.AppendBulk(stats)
	table.Render()

	return nil
}

func PruneHashTrieNodeInDataBase(db ethdb.Database) error {
	it := db.NewIterator([]byte{}, []byte{})
	defer it.Release()

	total_num := 0
	for it.Next() {
		var key = it.Key()
		switch {
		case IsLegacyTrieNode(key, it.Value()):
			db.Delete(key)
			total_num++
			if total_num%100000 == 0 {
				log.Info("Pruning hash-base state trie nodes", "Complete progress: ", total_num)
			}
		default:
			continue
		}
	}
	log.Info("Pruning hash-base state trie nodes", "Complete progress", total_num)
	return nil
}

// InspectDatabase traverses the entire database and checks the size
// of all different categories of data.
func InspectDatabase(db ethdb.Database, keyPrefix, keyStart []byte) error {
	it := db.NewIterator(keyPrefix, keyStart)
	defer it.Release()

	var (
		count  int64
		start  = time.Now()
		logged = time.Now()

		// Key-value store statistics
		headers         stat
		bodies          stat
		receipts        stat
		tds             stat
		numHashPairings stat
		hashNumPairings stat
		legacyTries     stat
		stateLookups    stat
		accountTries    stat
		storageTries    stat
		codes           stat
		txLookups       stat
		accountSnaps    stat
		storageSnaps    stat
		preimages       stat
		bloomBits       stat
		cliqueSnaps     stat
		parliaSnaps     stat

		// Les statistic
		chtTrieNodes   stat
		bloomTrieNodes stat

		// Meta- and unaccounted data
		metadata    stat
		unaccounted stat

		// Totals
		total common.StorageSize
	)
	// Inspect key-value database first.
	for it.Next() {
		var (
			key  = it.Key()
			size = common.StorageSize(len(key) + len(it.Value()))
		)
		total += size
		switch {
		case bytes.HasPrefix(key, headerPrefix) && len(key) == (len(headerPrefix)+8+common.HashLength):
			headers.Add(size)
		case bytes.HasPrefix(key, blockBodyPrefix) && len(key) == (len(blockBodyPrefix)+8+common.HashLength):
			bodies.Add(size)
		case bytes.HasPrefix(key, blockReceiptsPrefix) && len(key) == (len(blockReceiptsPrefix)+8+common.HashLength):
			receipts.Add(size)
		case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerTDSuffix):
			tds.Add(size)
		case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerHashSuffix):
			numHashPairings.Add(size)
		case bytes.HasPrefix(key, headerNumberPrefix) && len(key) == (len(headerNumberPrefix)+common.HashLength):
			hashNumPairings.Add(size)
		case IsLegacyTrieNode(key, it.Value()):
			legacyTries.Add(size)
		case bytes.HasPrefix(key, stateIDPrefix) && len(key) == len(stateIDPrefix)+common.HashLength:
			stateLookups.Add(size)
		case IsAccountTrieNode(key):
			accountTries.Add(size)
		case IsStorageTrieNode(key):
			storageTries.Add(size)
		case bytes.HasPrefix(key, CodePrefix) && len(key) == len(CodePrefix)+common.HashLength:
			codes.Add(size)
		case bytes.HasPrefix(key, txLookupPrefix) && len(key) == (len(txLookupPrefix)+common.HashLength):
			txLookups.Add(size)
		case bytes.HasPrefix(key, SnapshotAccountPrefix) && len(key) == (len(SnapshotAccountPrefix)+common.HashLength):
			accountSnaps.Add(size)
		case bytes.HasPrefix(key, SnapshotStoragePrefix) && len(key) == (len(SnapshotStoragePrefix)+2*common.HashLength):
			storageSnaps.Add(size)
		case bytes.HasPrefix(key, PreimagePrefix) && len(key) == (len(PreimagePrefix)+common.HashLength):
			preimages.Add(size)
		case bytes.HasPrefix(key, configPrefix) && len(key) == (len(configPrefix)+common.HashLength):
			metadata.Add(size)
		case bytes.HasPrefix(key, genesisPrefix) && len(key) == (len(genesisPrefix)+common.HashLength):
			metadata.Add(size)
		case bytes.HasPrefix(key, bloomBitsPrefix) && len(key) == (len(bloomBitsPrefix)+10+common.HashLength):
			bloomBits.Add(size)
		case bytes.HasPrefix(key, BloomBitsIndexPrefix):
			bloomBits.Add(size)
		case bytes.HasPrefix(key, CliqueSnapshotPrefix) && len(key) == 7+common.HashLength:
			cliqueSnaps.Add(size)
		case bytes.HasPrefix(key, ParliaSnapshotPrefix) && len(key) == 7+common.HashLength:
			parliaSnaps.Add(size)
		case bytes.HasPrefix(key, ChtTablePrefix) ||
			bytes.HasPrefix(key, ChtIndexTablePrefix) ||
			bytes.HasPrefix(key, ChtPrefix): // Canonical hash trie
			chtTrieNodes.Add(size)
		case bytes.HasPrefix(key, BloomTrieTablePrefix) ||
			bytes.HasPrefix(key, BloomTrieIndexPrefix) ||
			bytes.HasPrefix(key, BloomTriePrefix): // Bloomtrie sub
			bloomTrieNodes.Add(size)
		default:
			var accounted bool
			for _, meta := range [][]byte{
				databaseVersionKey, headHeaderKey, headBlockKey, headFastBlockKey,
				lastPivotKey, fastTrieProgressKey, snapshotDisabledKey, SnapshotRootKey, snapshotJournalKey,
				snapshotGeneratorKey, snapshotRecoveryKey, txIndexTailKey, fastTxLookupLimitKey,
				uncleanShutdownKey, badBlockKey, transitionStatusKey, skeletonSyncStatusKey,
				persistentStateIDKey, trieJournalKey, snapshotSyncStatusKey,
			} {
				if bytes.Equal(key, meta) {
					metadata.Add(size)
					accounted = true
					break
				}
			}
			if !accounted {
				unaccounted.Add(size)
			}
		}
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}
	// Display the database statistic of key-value store.
	stats := [][]string{
		{"Key-Value store", "Headers", headers.Size(), headers.Count()},
		{"Key-Value store", "Bodies", bodies.Size(), bodies.Count()},
		{"Key-Value store", "Receipt lists", receipts.Size(), receipts.Count()},
		{"Key-Value store", "Difficulties", tds.Size(), tds.Count()},
		{"Key-Value store", "Block number->hash", numHashPairings.Size(), numHashPairings.Count()},
		{"Key-Value store", "Block hash->number", hashNumPairings.Size(), hashNumPairings.Count()},
		{"Key-Value store", "Transaction index", txLookups.Size(), txLookups.Count()},
		{"Key-Value store", "Bloombit index", bloomBits.Size(), bloomBits.Count()},
		{"Key-Value store", "Contract codes", codes.Size(), codes.Count()},
		{"Key-Value store", "Hash trie nodes", legacyTries.Size(), legacyTries.Count()},
		{"Key-Value store", "Path trie state lookups", stateLookups.Size(), stateLookups.Count()},
		{"Key-Value store", "Path trie account nodes", accountTries.Size(), accountTries.Count()},
		{"Key-Value store", "Path trie storage nodes", storageTries.Size(), storageTries.Count()},
		{"Key-Value store", "Trie preimages", preimages.Size(), preimages.Count()},
		{"Key-Value store", "Account snapshot", accountSnaps.Size(), accountSnaps.Count()},
		{"Key-Value store", "Storage snapshot", storageSnaps.Size(), storageSnaps.Count()},
		{"Key-Value store", "Clique snapshots", cliqueSnaps.Size(), cliqueSnaps.Count()},
		{"Key-Value store", "Parlia snapshots", parliaSnaps.Size(), parliaSnaps.Count()},
		{"Key-Value store", "Singleton metadata", metadata.Size(), metadata.Count()},
		{"Light client", "CHT trie nodes", chtTrieNodes.Size(), chtTrieNodes.Count()},
		{"Light client", "Bloom trie nodes", bloomTrieNodes.Size(), bloomTrieNodes.Count()},
	}
	// Inspect all registered append-only file store then.
	ancients, err := inspectFreezers(db)
	if err != nil {
		return err
	}
	for _, ancient := range ancients {
		for _, table := range ancient.sizes {
			stats = append(stats, []string{
				fmt.Sprintf("Ancient store (%s)", strings.Title(ancient.name)),
				strings.Title(table.name),
				table.size.String(),
				fmt.Sprintf("%d", ancient.count()),
			})
		}
		total += ancient.size()
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Database", "Category", "Size", "Items"})
	table.SetFooter([]string{"", "Total", total.String(), " "})
	table.AppendBulk(stats)
	table.Render()

	if unaccounted.size > 0 {
		log.Error("Database contains unaccounted data", "size", unaccounted.size, "count", unaccounted.count)
	}
	showLeveldbStats(db)
	return nil
}

func InspectStateDatabase(db ethdb.KeyValueStore, keyPrefix, keyStart []byte) error {
	it := db.NewIterator(keyPrefix, keyStart)
	defer it.Release()

	var (
		count  int64
		start  = time.Now()
		logged = time.Now()

		// Key-value store statistics
		headers         stat
		bodies          stat
		receipts        stat
		tds             stat
		numHashPairings stat
		hashNumPairings stat
		legacyTries     stat
		stateLookups    stat
		accountTries    stat
		storageTries    stat
		codes           stat
		txLookups       stat
		accountSnaps    stat
		storageSnaps    stat
		preimages       stat
		bloomBits       stat
		cliqueSnaps     stat
		parliaSnaps     stat

		// Les statistic
		chtTrieNodes   stat
		bloomTrieNodes stat

		// Meta- and unaccounted data
		metadata    stat
		unaccounted stat

		// Totals
		total common.StorageSize
	)
	// Inspect key-value database first.
	for it.Next() {
		var (
			key  = it.Key()
			size = common.StorageSize(len(key) + len(it.Value()))
		)
		total += size
		switch {
		case bytes.HasPrefix(key, headerPrefix) && len(key) == (len(headerPrefix)+8+common.HashLength):
			headers.Add(size)
		case bytes.HasPrefix(key, blockBodyPrefix) && len(key) == (len(blockBodyPrefix)+8+common.HashLength):
			bodies.Add(size)
		case bytes.HasPrefix(key, blockReceiptsPrefix) && len(key) == (len(blockReceiptsPrefix)+8+common.HashLength):
			receipts.Add(size)
		case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerTDSuffix):
			tds.Add(size)
		case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerHashSuffix):
			numHashPairings.Add(size)
		case bytes.HasPrefix(key, headerNumberPrefix) && len(key) == (len(headerNumberPrefix)+common.HashLength):
			hashNumPairings.Add(size)
		case IsLegacyTrieNode(key, it.Value()):
			legacyTries.Add(size)
		case bytes.HasPrefix(key, stateIDPrefix) && len(key) == len(stateIDPrefix)+common.HashLength:
			stateLookups.Add(size)
		case IsAccountTrieNode(key):
			accountTries.Add(size)
		case IsStorageTrieNode(key):
			storageTries.Add(size)
		case bytes.HasPrefix(key, CodePrefix) && len(key) == len(CodePrefix)+common.HashLength:
			codes.Add(size)
		case bytes.HasPrefix(key, txLookupPrefix) && len(key) == (len(txLookupPrefix)+common.HashLength):
			txLookups.Add(size)
		case bytes.HasPrefix(key, SnapshotAccountPrefix) && len(key) == (len(SnapshotAccountPrefix)+common.HashLength):
			accountSnaps.Add(size)
		case bytes.HasPrefix(key, SnapshotStoragePrefix) && len(key) == (len(SnapshotStoragePrefix)+2*common.HashLength):
			storageSnaps.Add(size)
		case bytes.HasPrefix(key, PreimagePrefix) && len(key) == (len(PreimagePrefix)+common.HashLength):
			preimages.Add(size)
		case bytes.HasPrefix(key, configPrefix) && len(key) == (len(configPrefix)+common.HashLength):
			metadata.Add(size)
		case bytes.HasPrefix(key, genesisPrefix) && len(key) == (len(genesisPrefix)+common.HashLength):
			metadata.Add(size)
		case bytes.HasPrefix(key, bloomBitsPrefix) && len(key) == (len(bloomBitsPrefix)+10+common.HashLength):
			bloomBits.Add(size)
		case bytes.HasPrefix(key, BloomBitsIndexPrefix):
			bloomBits.Add(size)
		case bytes.HasPrefix(key, CliqueSnapshotPrefix) && len(key) == 7+common.HashLength:
			cliqueSnaps.Add(size)
		case bytes.HasPrefix(key, ParliaSnapshotPrefix) && len(key) == 7+common.HashLength:
			parliaSnaps.Add(size)
		case bytes.HasPrefix(key, ChtTablePrefix) ||
			bytes.HasPrefix(key, ChtIndexTablePrefix) ||
			bytes.HasPrefix(key, ChtPrefix): // Canonical hash trie
			chtTrieNodes.Add(size)
		case bytes.HasPrefix(key, BloomTrieTablePrefix) ||
			bytes.HasPrefix(key, BloomTrieIndexPrefix) ||
			bytes.HasPrefix(key, BloomTriePrefix): // Bloomtrie sub
			bloomTrieNodes.Add(size)
		default:
			var accounted bool
			for _, meta := range [][]byte{
				databaseVersionKey, headHeaderKey, headBlockKey, headFastBlockKey,
				lastPivotKey, fastTrieProgressKey, snapshotDisabledKey, SnapshotRootKey, snapshotJournalKey,
				snapshotGeneratorKey, snapshotRecoveryKey, txIndexTailKey, fastTxLookupLimitKey,
				uncleanShutdownKey, badBlockKey, transitionStatusKey, skeletonSyncStatusKey,
				persistentStateIDKey, trieJournalKey, snapshotSyncStatusKey,
			} {
				if bytes.Equal(key, meta) {
					metadata.Add(size)
					accounted = true
					break
				}
			}
			if !accounted {
				unaccounted.Add(size)
			}
		}
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Inspecting database", "count", count, "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}
	// Display the database statistic of key-value store.
	stats := [][]string{
		{"Key-Value store", "Headers", headers.Size(), headers.Count()},
		{"Key-Value store", "Bodies", bodies.Size(), bodies.Count()},
		{"Key-Value store", "Receipt lists", receipts.Size(), receipts.Count()},
		{"Key-Value store", "Difficulties", tds.Size(), tds.Count()},
		{"Key-Value store", "Block number->hash", numHashPairings.Size(), numHashPairings.Count()},
		{"Key-Value store", "Block hash->number", hashNumPairings.Size(), hashNumPairings.Count()},
		{"Key-Value store", "Transaction index", txLookups.Size(), txLookups.Count()},
		{"Key-Value store", "Bloombit index", bloomBits.Size(), bloomBits.Count()},
		{"Key-Value store", "Contract codes", codes.Size(), codes.Count()},
		{"Key-Value store", "Hash trie nodes", legacyTries.Size(), legacyTries.Count()},
		{"Key-Value store", "Path trie state lookups", stateLookups.Size(), stateLookups.Count()},
		{"Key-Value store", "Path trie account nodes", accountTries.Size(), accountTries.Count()},
		{"Key-Value store", "Path trie storage nodes", storageTries.Size(), storageTries.Count()},
		{"Key-Value store", "Trie preimages", preimages.Size(), preimages.Count()},
		{"Key-Value store", "Account snapshot", accountSnaps.Size(), accountSnaps.Count()},
		{"Key-Value store", "Storage snapshot", storageSnaps.Size(), storageSnaps.Count()},
		{"Key-Value store", "Clique snapshots", cliqueSnaps.Size(), cliqueSnaps.Count()},
		{"Key-Value store", "Parlia snapshots", parliaSnaps.Size(), parliaSnaps.Count()},
		{"Key-Value store", "Singleton metadata", metadata.Size(), metadata.Count()},
		{"Light client", "CHT trie nodes", chtTrieNodes.Size(), chtTrieNodes.Count()},
		{"Light client", "Bloom trie nodes", bloomTrieNodes.Size(), bloomTrieNodes.Count()},
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Database", "Category", "Size", "Items"})
	table.SetFooter([]string{"", "Total", total.String(), " "})
	table.AppendBulk(stats)
	table.Render()

	if unaccounted.size > 0 {
		log.Error("Database contains unaccounted data", "size", unaccounted.size, "count", unaccounted.count)
	}
	showLeveldbStats(db)
	return nil
}

func MigrateSharding(db ethdb.Database, keyPrefix, keyStart []byte) error {
	sharded := db.Sharded()
	if !sharded || db.ShardNum() == 0 {
		log.Warn("cannot migrate into a un-sharded db!!")
		return nil
	}
	fmt.Println("main db stats....")
	showLeveldbStats(db)
	it := db.NewIterator(keyPrefix, keyStart)
	defer it.Release()

	var (
		count        int64
		start        = time.Now()
		logged       = time.Now()
		accountTries stat
		storageTries stat
		accountSnaps stat
		storageSnaps stat

		// Totals
		total common.StorageSize
	)
	// Inspect key-value database first.
	for it.Next() {
		var (
			key  = it.Key()
			val  = it.Value()
			size = common.StorageSize(len(key) + len(val))
		)
		total += size
		switch {
		case IsAccountTrieNode(key):
			accountTries.Add(size)
			_, p := ResolveAccountTrieNodeKey(key)
			DeleteAccountTrieNode(db, p)
			WriteAccountTrieNode(TryShardingByHash(db, common.Hash{}), p, val)
		case IsStorageTrieNode(key):
			storageTries.Add(size)
			_, h, p := ResolveStorageTrieNode(key)
			DeleteStorageTrieNode(db, h, p)
			WriteStorageTrieNode(TryShardingByHash(db, h), h, p, val)
		case bytes.HasPrefix(key, SnapshotAccountPrefix) && len(key) == (len(SnapshotAccountPrefix)+common.HashLength):
			accountSnaps.Add(size)
			h := common.BytesToHash(key[len(SnapshotAccountPrefix):])
			DeleteAccountSnapshot(db, h)
			WriteAccountSnapshot(TryShardingByHash(db, common.Hash{}), h, val)
		case bytes.HasPrefix(key, SnapshotStoragePrefix) && len(key) == (len(SnapshotStoragePrefix)+2*common.HashLength):
			storageSnaps.Add(size)
			h := common.BytesToHash(key[len(SnapshotAccountPrefix):(len(SnapshotAccountPrefix) + common.HashLength)])
			s := common.BytesToHash(key[(len(SnapshotAccountPrefix) + common.HashLength):(len(SnapshotAccountPrefix) + 2*common.HashLength)])
			DeleteStorageSnapshot(db, h, s)
			WriteStorageSnapshot(TryShardingByHash(db, h), h, s, val)
		}
		count++
		if count%1000 == 0 && time.Since(logged) > 8*time.Second {
			log.Info("Migrating database", "count", count, "accountTrie", accountTries.Count(),
				"storageTrie", storageTries.Count(), "accountSnap", accountSnaps.Count(),
				"storageSnap", storageSnaps.Count(), "elapsed", common.PrettyDuration(time.Since(start)))
			logged = time.Now()
		}
	}
	// Display the database statistic of key-value store.
	stats := [][]string{
		{"Key-Value store", "Path trie account nodes", accountTries.Size(), accountTries.Count()},
		{"Key-Value store", "Path trie storage nodes", storageTries.Size(), storageTries.Count()},
		{"Key-Value store", "Account snapshot", accountSnaps.Size(), accountSnaps.Count()},
		{"Key-Value store", "Storage snapshot", storageSnaps.Size(), storageSnaps.Count()},
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Database", "Category", "Size", "Items"})
	table.SetFooter([]string{"", "Total", total.String(), " "})
	table.AppendBulk(stats)
	table.Render()

	StatsAndCompact(db)
	return nil
}

func StatsAndCompact(db ethdb.Database) {
	fmt.Println("db stats...", "main db")
	showLeveldbStats(db)
	for i := uint64(0); i < db.ShardNum(); i++ {
		fmt.Println("db stats...", "shard", i)
		showLeveldbStats(db.Shard(i))
	}

	fmt.Println("db compacting...", "main db")
	compactDB(db)
	for i := uint64(0); i < db.ShardNum(); i++ {
		fmt.Println("db compacting...", "shard", i)
		compactDB(db.Shard(i))
	}

	fmt.Println("db stats...", "main db")
	showLeveldbStats(db)
	for i := uint64(0); i < db.ShardNum(); i++ {
		fmt.Println("db stats...", "shard", i)
		showLeveldbStats(db.Shard(i))
	}
}

func compactDB(db ethdb.KeyValueStore) error {
	cstart := time.Now()
	for b := 0x00; b <= 0xf0; b += 0x10 {
		var (
			start = []byte{byte(b)}
			end   = []byte{byte(b + 0x10)}
		)
		if b == 0xf0 {
			end = nil
		}
		log.Info("Compacting database", "range", fmt.Sprintf("%#x-%#x", start, end), "elapsed", common.PrettyDuration(time.Since(cstart)))
		if err := db.Compact(start, end); err != nil {
			log.Error("Database compaction failed", "error", err)
			return err
		}
	}
	return nil
}

func showLeveldbStats(db ethdb.KeyValueStater) {
	if stats, err := db.Stat("leveldb.stats"); err != nil {
		log.Warn("Failed to read database stats", "error", err)
	} else {
		fmt.Println(stats)
	}
	if ioStats, err := db.Stat("leveldb.iostats"); err != nil {
		log.Warn("Failed to read database iostats", "error", err)
	} else {
		fmt.Println(ioStats)
	}
}

// printChainMetadata prints out chain metadata to stderr.
func printChainMetadata(db ethdb.KeyValueStore) {
	fmt.Fprintf(os.Stderr, "Chain metadata\n")
	for _, v := range ReadChainMetadata(db) {
		fmt.Fprintf(os.Stderr, "  %s\n", strings.Join(v, ": "))
	}
	fmt.Fprintf(os.Stderr, "\n\n")
}

// ReadChainMetadata returns a set of key/value pairs that contains informatin
// about the database chain status. This can be used for diagnostic purposes
// when investigating the state of the node.
func ReadChainMetadata(db ethdb.KeyValueStore) [][]string {
	pp := func(val *uint64) string {
		if val == nil {
			return "<nil>"
		}
		return fmt.Sprintf("%d (%#x)", *val, *val)
	}
	data := [][]string{
		{"databaseVersion", pp(ReadDatabaseVersion(db))},
		{"headBlockHash", fmt.Sprintf("%v", ReadHeadBlockHash(db))},
		{"headFastBlockHash", fmt.Sprintf("%v", ReadHeadFastBlockHash(db))},
		{"headHeaderHash", fmt.Sprintf("%v", ReadHeadHeaderHash(db))},
		{"lastPivotNumber", pp(ReadLastPivotNumber(db))},
		{"len(snapshotSyncStatus)", fmt.Sprintf("%d bytes", len(ReadSnapshotSyncStatus(db)))},
		{"snapshotDisabled", fmt.Sprintf("%v", ReadSnapshotDisabled(db))},
		{"snapshotJournal", fmt.Sprintf("%d bytes", len(ReadSnapshotJournal(db)))},
		{"snapshotRecoveryNumber", pp(ReadSnapshotRecoveryNumber(db))},
		{"snapshotRoot", fmt.Sprintf("%v", ReadSnapshotRoot(db))},
		{"txIndexTail", pp(ReadTxIndexTail(db))},
		{"fastTxLookupLimit", pp(ReadFastTxLookupLimit(db))},
	}
	return data
}

func TryShardingByHash(db ethdb.Database, h common.Hash) ethdb.KeyValueStore {
	if !db.Sharded() {
		return db
	}
	return db.ShardByHash(h)
}
