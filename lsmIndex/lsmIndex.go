package lsmIndex

import (
	"bitcask/config/constants"
	"bitcask/logger"
	"bitcask/lsmIndex/memTable"
	"bitcask/lsmIndex/ssTable"
	"bitcask/lsmIndex/ssTableWriter"
	"bitcask/utils"
	"fmt"
	"sync"
)

type LsmIndex struct {
	primaryMemTable       *memTable.MemTable
	secondaryMemTableList []*memTable.MemTable
	ssTableList           []*ssTable.SSTable

	writeMutex                      *sync.RWMutex
	secondaryMemTableListWriteMutex *sync.RWMutex
	ssTableListWriteMutex           *sync.RWMutex

	ssTableWriter      *ssTableWriter.SSTableWriter
	removeMemTableChan chan *memTable.MemTable

	insertSSTableChan chan *ssTable.SSTable

	dataDirectory string

	isInitialized bool
}

func (lsmIndex *LsmIndex) GetOrPanic(key string) string {
	if val, isFound := lsmIndex.Get(key); isFound {
		return val
	} else {
		panic(constants.NotFoundMsg)
	}
}

func (lsmIndex *LsmIndex) Get(key string) (string, bool) {
	if val, isFound := lsmIndex.getFromMemTables(key); isFound {
		return val, isFound
	}

	if val, isFound := lsmIndex.getFromSSTables(key); isFound {
		return val, isFound
	}

	return "", false
}

func (lsmIndex *LsmIndex) getFromMemTables(key string) (string, bool) {
	memTables := lsmIndex.getAllMemTables()

	for _, memTable_ := range memTables {
		if val, isFound := memTable_.Get(key); isFound {
			return val, true
		}
	}

	return "", false
}

func (lsmIndex *LsmIndex) getFromSSTables(key string) (string, bool) {
	lsmIndex.ssTableListWriteMutex.Lock()

	size := len(lsmIndex.ssTableList)
	var i int

	for i = size - 1; i >= 0; i-- {
		ssTable_ := lsmIndex.ssTableList[i]

		if val, isFound := ssTable_.Get(key); isFound {
			return val, true
		}
	}

	lsmIndex.ssTableListWriteMutex.Unlock()
	return "", false
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

	lsmIndex.secondaryMemTableList = append(lsmIndex.secondaryMemTableList, memTable)

	lsmIndex.secondaryMemTableListWriteMutex.Unlock()
}

func (lsmIndex *LsmIndex) removeMemTableFromSecondaryList(memTableToRemove *memTable.MemTable) {
	lsmIndex.secondaryMemTableListWriteMutex.Lock()

	var secondaryMemTables []*memTable.MemTable

	for _, memTable_ := range lsmIndex.secondaryMemTableList {
		if memTable_ != memTableToRemove {
			secondaryMemTables = append(secondaryMemTables, memTable_)
		}
	}

	lsmIndex.secondaryMemTableList = secondaryMemTables

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
	lsmIndex.ssTableListWriteMutex = &sync.RWMutex{}

	lsmIndex.removeMemTableChan = make(chan *memTable.MemTable)
	lsmIndex.insertSSTableChan = make(chan *ssTable.SSTable)

	lsmIndex.ssTableWriter = &ssTableWriter.SSTableWriter{
		SuccessChan:    &lsmIndex.removeMemTableChan,
		DataDirectory:  lsmIndex.dataDirectory,
		NewSSTableChan: &lsmIndex.insertSSTableChan,
	}
	lsmIndex.ssTableWriter.Init()

	lsmIndex.consumeSuccessChan()
	lsmIndex.consumeInsertSSTableChan()

	lsmIndex.isInitialized = true
}

func (lsmIndex *LsmIndex) GetDataDirectory() string {
	return lsmIndex.dataDirectory
}

func (lsmIndex *LsmIndex) getAllMemTables() []*memTable.MemTable {
	var memTables []*memTable.MemTable

	if lsmIndex.secondaryMemTableList == nil {
		memTables = []*memTable.MemTable{lsmIndex.primaryMemTable}
	} else {
		memTables = append(lsmIndex.secondaryMemTableList, lsmIndex.primaryMemTable)
	}

	return memTables
}

func (lsmIndex *LsmIndex) consumeSuccessChan() {
	logger.SugaredLogger.Infof("Starting Memtable cleanup go routine for channel %v", lsmIndex.removeMemTableChan)

	go func() {
		for {
			select {
			case memTable_ := <-lsmIndex.removeMemTableChan:
				logger.SugaredLogger.Infof("Removing memtable %v", memTable_)
				lsmIndex.removeMemTableFromSecondaryList(memTable_)
				logger.SugaredLogger.Infof("Removed memtable %v", memTable_)
			}
		}
	}()
}

func (lsmIndex *LsmIndex) consumeInsertSSTableChan() {
	logger.SugaredLogger.Infof("Starting SSTable insertion go routine for channel %v", lsmIndex.insertSSTableChan)

	go func() {
		for {
			select {
			case ssTable_ := <-lsmIndex.insertSSTableChan:
				logger.SugaredLogger.Infof("Inserting SSTable %v", ssTable_)
				lsmIndex.insertNewSSTable(ssTable_)
				logger.SugaredLogger.Infof("Inserted SSTable %v", ssTable_)
			}
		}
	}()
}

func (lsmIndex *LsmIndex) insertNewSSTable(ssTable *ssTable.SSTable) {
	lsmIndex.ssTableListWriteMutex.Lock()

	lsmIndex.ssTableList = append(lsmIndex.ssTableList, ssTable)

	lsmIndex.ssTableListWriteMutex.Unlock()
}

func NewLsmIndex() (*LsmIndex, error) {
	lsmIndex := LsmIndex{
		primaryMemTable:       nil,
		secondaryMemTableList: nil,
		dataDirectory:         utils.GetDataDirectory(),
	}

	lsmIndex.Init()

	return &lsmIndex, nil
}
