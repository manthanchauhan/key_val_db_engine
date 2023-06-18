package lsmIndex

import (
	"bitcask/config/constants"
	"bitcask/logger"
	"bitcask/lsmIndex/memTable"
	"bitcask/lsmIndex/ssTableWriter"
	"bitcask/utils"
	"fmt"
	"sync"
)

type LsmIndex struct {
	primaryMemTable    *memTable.MemTable
	secondaryMemTables []*memTable.MemTable

	writeMutex                      *sync.RWMutex
	secondaryMemTableListWriteMutex *sync.RWMutex

	ssTableWriter            *ssTableWriter.SSTableWriter
	ssTableWriterSuccessChan chan *memTable.MemTable

	dataDirectory string

	isInitialized bool
}

func (lsmIndex *LsmIndex) GetOrPanic(key string) string {

	memTables := lsmIndex.getAllMemTables()

	for _, memTable_ := range memTables {
		if val, isFound := memTable_.Get(key); isFound {
			return val
		}
	}

	panic(constants.NotFoundMsg)
}

func (lsmIndex *LsmIndex) GetDataLocation(key string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

func (lsmIndex *LsmIndex) Set(key string, val string) error {
	lsmIndex.writeMutex.Lock()

	err := lsmIndex.setWithoutThreadSafe(key, val)

	lsmIndex.writeMutex.Unlock()

	return err
}

func (lsmIndex *LsmIndex) setWithoutThreadSafe(key string, val string) error {
	err := lsmIndex.primaryMemTable.Put(key, val)
	if err != nil {
		return err
	}

	logger.SugaredLogger.Infof("key '%s' written to MemTable %v", key, lsmIndex.primaryMemTable)

	isFull, err := lsmIndex.primaryMemTable.IsFull()
	if err != nil {
		return err
	}

	if isFull {
		logger.SugaredLogger.Infof("MemTable %v is full", lsmIndex.primaryMemTable)

		var temp = lsmIndex.primaryMemTable

		lsmIndex.addMemTableToSecondaryList(lsmIndex.primaryMemTable)
		logger.SugaredLogger.Infof("MemTable %v added to secondary memtables", lsmIndex.primaryMemTable)

		lsmIndex.primaryMemTable, err = memTable.NewMemTable()
		if err != nil {
			return err
		}
		logger.SugaredLogger.Infof("Primary memtable %v is replaced with new memtable %v", temp, lsmIndex.primaryMemTable)

		lsmIndex.ssTableWriter.WriteMemTableToSSTable(temp)
	}

	return nil
}

func (lsmIndex *LsmIndex) addMemTableToSecondaryList(memTable *memTable.MemTable) {
	lsmIndex.secondaryMemTableListWriteMutex.Lock()

	lsmIndex.secondaryMemTables = append(lsmIndex.secondaryMemTables, memTable)

	lsmIndex.secondaryMemTableListWriteMutex.Unlock()
}

func (lsmIndex *LsmIndex) removeMemTableFromSecondaryList(memTableToRemove *memTable.MemTable) {
	lsmIndex.secondaryMemTableListWriteMutex.Lock()

	var secondaryMemTables []*memTable.MemTable

	for _, memTable_ := range lsmIndex.secondaryMemTables {
		if memTable_ != memTableToRemove {
			secondaryMemTables = append(secondaryMemTables, memTable_)
		}
	}

	lsmIndex.secondaryMemTables = secondaryMemTables

	lsmIndex.secondaryMemTableListWriteMutex.Unlock()
}

func (lsmIndex *LsmIndex) ImportDataSegment(fileName string, initValCheck func(k string) bool) {
	//TODO implement me
	panic("implement me")
}

func (lsmIndex *LsmIndex) Init() {
	if lsmIndex.isInitialized {
		panic(fmt.Sprintf("LSM Index %v is already initialized", lsmIndex))
	}

	var err error

	lsmIndex.primaryMemTable, err = memTable.NewMemTable()
	if err != nil {
		panic(err)
	}

	lsmIndex.writeMutex = &sync.RWMutex{}
	lsmIndex.secondaryMemTableListWriteMutex = &sync.RWMutex{}

	lsmIndex.ssTableWriterSuccessChan = make(chan *memTable.MemTable)

	lsmIndex.ssTableWriter = &ssTableWriter.SSTableWriter{
		SuccessChan:   &lsmIndex.ssTableWriterSuccessChan,
		DataDirectory: lsmIndex.dataDirectory,
	}
	lsmIndex.ssTableWriter.Init()

	lsmIndex.consumeSuccessChan()

	lsmIndex.isInitialized = true
}

func (lsmIndex *LsmIndex) GetDataDirectory() string {
	return lsmIndex.dataDirectory
}

func (lsmIndex *LsmIndex) getAllMemTables() []*memTable.MemTable {
	var memTables []*memTable.MemTable

	if lsmIndex.secondaryMemTables == nil {
		memTables = []*memTable.MemTable{lsmIndex.primaryMemTable}
	} else {
		memTables = append(lsmIndex.secondaryMemTables, lsmIndex.primaryMemTable)
	}

	return memTables
}

func (lsmIndex *LsmIndex) consumeSuccessChan() {
	logger.SugaredLogger.Infof("Starting Memtable cleanup go routine for channel %v", lsmIndex.ssTableWriterSuccessChan)
	go func() {
		for {
			select {
			case memTable_ := <-lsmIndex.ssTableWriterSuccessChan:
				logger.SugaredLogger.Infof("Removing memtable %v", memTable_)
				lsmIndex.removeMemTableFromSecondaryList(memTable_)
				logger.SugaredLogger.Infof("Removed memtable %v", memTable_)
			}
		}
	}()
}

func NewLsmIndex() (*LsmIndex, error) {
	lsmIndex := LsmIndex{
		primaryMemTable:    nil,
		secondaryMemTables: nil,
		dataDirectory:      utils.GetDataDirectory(),
	}

	lsmIndex.Init()

	return &lsmIndex, nil
}
