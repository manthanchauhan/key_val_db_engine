package memTable

import (
	"bitcask/config/constants"
	"bitcask/utils"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"sync"
	"time"
)

type MemTable struct {
	Id           int64
	redBlackTree *redblacktree.Tree
	maxSize      int
	size         int

	putMutex *sync.RWMutex

	IsBeingWrittenToDisk bool
}

func (memTable *MemTable) Init() error {
	memTable.redBlackTree = redblacktree.NewWithStringComparator()
	return nil
}

func (memTable *MemTable) Put(key string, val string) error {
	defer utils.LockThenDefer(memTable.putMutex)()

	if memTable.IsBeingWrittenToDisk {
		panic(fmt.Sprintf("MemTable %v is being written to disk", memTable))
	}

	memTable.redBlackTree.Put(key, val)
	memTable.size += len(key) + len(val)
	return nil
}

func (memTable *MemTable) IsFull() (bool, error) {
	return memTable.Size() >= memTable.maxSize, nil
}

func (memTable *MemTable) Size() int {
	return memTable.size
}

func (memTable *MemTable) Get(key string) (string, bool) {
	val, isFound := memTable.redBlackTree.Get(key)
	return fmt.Sprintf("%v", val), isFound
}

func (memTable *MemTable) GetKeyValPairs() [][]string {
	kvPairs := make([][]string, memTable.redBlackTree.Size())

	it := memTable.redBlackTree.Iterator()

	for i := 0; it.Next(); i++ {
		k := it.Key()
		v := it.Value()

		kvPairs[i] = []string{fmt.Sprintf("%v", k), fmt.Sprintf("%v", v)}
	}

	return kvPairs
}

func (memTable *MemTable) String() string {
	return fmt.Sprintf("{Id:%d}", memTable.Id)
}

func NewMemTable() (*MemTable, error) {
	memTable := MemTable{
		redBlackTree:         nil,
		maxSize:              constants.MemTableMaxSizeBytes,
		size:                 0,
		putMutex:             &sync.RWMutex{},
		IsBeingWrittenToDisk: false,
		Id:                   time.Now().UnixNano(),
	}

	if err := memTable.Init(); err != nil {
		return nil, err
	}

	return &memTable, nil
}
