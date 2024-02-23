package lsmIndex

import (
	"bitcask/config/constants"
	"bitcask/dataIO/index/hashIndex/disk"
	"bitcask/dataIO/index/lsmIndex/memTable"
	"bitcask/dataIO/index/lsmIndex/ssTable"
	"bitcask/dataIO/index/lsmIndex/ssTableWriter"
	"bitcask/logger"
	"bitcask/utils"
	"errors"
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

func (lsmIndex *LsmIndex) Get(key string) (string, error) {
	if val, isFound := lsmIndex.getFromMemTables(key); isFound {
		return val, nil
	}

	if val, isFound := lsmIndex.getFromSSTables(key); isFound {
		return val, nil
	}

	return "", errors.New(constants.ErrMsgNotFound)
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
	defer utils.LockThenDefer(lsmIndex.ssTableListWriteMutex)()

	size := len(lsmIndex.ssTableList)
	var i int

	for i = size - 1; i >= 0; i-- {
		ssTable_ := lsmIndex.ssTableList[i]

		if val, isFound := ssTable_.Get(key); isFound {
			return val, true
		}
	}

	return "", false
}

func (lsmIndex *LsmIndex) GetDataLocation(key string) (string, bool) {
	//TODO implement me
	panic("implement me")
}

func (lsmIndex *LsmIndex) Set(key string, val string) error {
	defer utils.LockThenDefer(lsmIndex.writeMutex)()

	err := lsmIndex.setWithoutThreadSafe(key, val)

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
	defer utils.LockThenDefer(lsmIndex.secondaryMemTableListWriteMutex)()

	lsmIndex.secondaryMemTableList = append(lsmIndex.secondaryMemTableList, memTable)
}

func (lsmIndex *LsmIndex) removeMemTableFromSecondaryList(memTableToRemove *memTable.MemTable) {
	defer utils.LockThenDefer(lsmIndex.secondaryMemTableListWriteMutex)()

	var secondaryMemTables []*memTable.MemTable

	for _, memTable_ := range lsmIndex.secondaryMemTableList {
		if memTable_ != memTableToRemove {
			secondaryMemTables = append(secondaryMemTables, memTable_)
		}
	}

	lsmIndex.secondaryMemTableList = secondaryMemTables
}

func (lsmIndex *LsmIndex) ImportData() {
	dataSegmentFileNames := disk.GetDataSegmentFileNameList(lsmIndex.dataDirectory)

	for _, fileName := range dataSegmentFileNames {
		lsmIndex.ImportDataSegment(fileName)
	}

	walFileNames := disk.GetDataSegmentFileNameList(constants.MemTableWALDirectory)

	var filteredWALNames []string

	for _, fileName := range walFileNames {
		if lsmIndex.primaryMemTable.WalFileName == fileName {
			continue
		}

		isTaken := false

		for _, memTable_ := range lsmIndex.secondaryMemTableList {
			if memTable_.WalFileName == fileName {
				isTaken = true
				break
			}
		}

		if isTaken {
			continue
		}

		filteredWALNames = append(filteredWALNames, fileName)
	}

	for _, fileName := range filteredWALNames {
		lsmIndex.ImportWAL(fileName)
	}

	lsmIndex.flushSecondaryMemTableList()
}

func (lsmIndex *LsmIndex) flushSecondaryMemTableList() {
	for _, memTable_ := range lsmIndex.secondaryMemTableList {
		lsmIndex.ssTableWriter.WriteMemTableToSSTable(memTable_)
	}
}

func (lsmIndex *LsmIndex) ImportDataSegment(fileName string) {
	ssTable_ := ssTable.NewSSTableFromFileName(fileName, lsmIndex.dataDirectory)
	lsmIndex.insertNewSSTable(ssTable_)
}

func (lsmIndex *LsmIndex) ImportWAL(fileName string) {
	memTable_, err := memTable.FromWAL(fileName, constants.MemTableWALDirectory)
	if err != nil {
		panic(err)
	}

	lsmIndex.addMemTableToSecondaryList(memTable_)
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

	lsmIndex.ImportData()
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
	defer utils.LockThenDefer(lsmIndex.ssTableListWriteMutex)()

	lsmIndex.ssTableList = append(lsmIndex.ssTableList, ssTable)
}

func (lsmIndex *LsmIndex) Delete(key string) error {
	//TODO implement me
	panic("implement me")
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
