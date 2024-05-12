package memTable

import (
	"bitcask/config/constants"
	"bitcask/dataIO/dataSegment"
	"bitcask/dataIO/index/hashIndex/disk"
	"bitcask/logger"
	"bitcask/utils"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"os"
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
	WalFileName          string
	walDirectory         string
}

func (memTable *MemTable) Init() error {
	memTable.redBlackTree = redblacktree.NewWithStringComparator()
	return nil
}

func (memTable *MemTable) Put(key string, val string) error {
	defer utils.LockThenDefer(memTable.putMutex)()

	if err := memTable.writeWAL(key, val); err != nil {
		panic(err)
	}

	return memTable.put(key, val)
}

func (memTable *MemTable) put(key string, val string) error {
	if memTable.IsBeingWrittenToDisk {
		panic(fmt.Sprintf("MemTable %v is being written to disk", memTable))
	}

	memTable.redBlackTree.Put(key, val)
	memTable.size += len(key) + len(val)
	return nil
}

func (memTable *MemTable) writeWAL(key string, val string) error {
	f, deferFunc := dataSegment.GetLogFile(memTable.walDirectory+"/"+memTable.WalFileName, os.O_APPEND|os.O_WRONLY)
	defer deferFunc(f)

	dataSegment.Write(key, val, f)

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

func (memTable *MemTable) deleteWAL() error {
	return os.Remove(memTable.walDirectory + "/" + memTable.WalFileName)
}

func (memTable *MemTable) IsWrittenToSSTable() {
	logger.SugaredLogger.Infof("Removing " + memTable.walDirectory + "/" + memTable.WalFileName)

	if err := memTable.deleteWAL(); err != nil {
		panic(err)
	}

	memTable.IsBeingWrittenToDisk = false
}

func NewMemTable() (*MemTable, error) {
	memTable := MemTable{
		redBlackTree:         nil,
		maxSize:              constants.MemTableMaxSizeBytes,
		size:                 0,
		putMutex:             &sync.RWMutex{},
		IsBeingWrittenToDisk: false,
		Id:                   time.Now().UnixNano(),
		WalFileName:          disk.CreateNewDataSegmentInDirectory(utils.GetDataDirectoryForIndex(constants.IndexTypeLSMIndex) + "/WALs"),
		walDirectory:         utils.GetDataDirectoryForIndex(constants.IndexTypeLSMIndex) + "/WALs",
	}

	if err := memTable.Init(); err != nil {
		return nil, err
	}

	return &memTable, nil
}

func FromWAL(fileName string, directory string) (*MemTable, error) {
	memTable := MemTable{
		redBlackTree:         nil,
		maxSize:              constants.MemTableMaxSizeBytes,
		size:                 0,
		putMutex:             &sync.RWMutex{},
		IsBeingWrittenToDisk: false,
		Id:                   time.Now().UnixNano(),
		WalFileName:          fileName,
		walDirectory:         directory,
	}

	if err := memTable.Init(); err != nil {
		return nil, err
	}

	disk.ParseDataSegment(fileName, directory, func(k string, v string, byteOffset int64) {
		if err := memTable.put(k, v); err != nil {
			panic(err)
		}
	})

	return &memTable, nil
}
