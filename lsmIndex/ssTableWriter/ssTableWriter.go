package ssTableWriter

import (
	"bitcask/disk"
	"bitcask/disk/dataSegment"
	"bitcask/logger"
	"bitcask/lsmIndex/memTable"
	"fmt"
	"os"
)

type SSTableWriter struct {
	SuccessChan *chan *memTable.MemTable
	jobChan     chan *memTable.MemTable

	DataDirectory string

	writeGoRoutine          func()
	isWriteGoRoutineRunning bool

	isInitialized bool
}

func (s *SSTableWriter) WriteMemTableToSSTable(table *memTable.MemTable) {
	s.jobChan <- table
	logger.SugaredLogger.Infof("MemTable %v queued for writing to SSTable", table)

}

func (s *SSTableWriter) Init() {
	if s.isInitialized {
		panic(fmt.Sprintf("Already initialized SSTableWriter-%s", *s))
	}

	s.jobChan = make(chan *memTable.MemTable)

	s.writeGoRoutine = func() {
		for {
			select {
			case memTable_ := <-s.jobChan:
				logger.SugaredLogger.Infof("Writing MemTable %v to SSTable", memTable_)

				s.writeMemTableToSSTable(memTable_)
				*s.SuccessChan <- memTable_

				logger.SugaredLogger.Infof("MemTable %v queued for destruction", memTable_)
			}
		}
	}

	if !s.isWriteGoRoutineRunning {
		go s.writeGoRoutine()
	}

	s.isInitialized = true
}

func (s *SSTableWriter) writeMemTableToSSTable(memTable *memTable.MemTable) {
	memTable.IsBeingWrittenToDisk = true

	ssTableFileName := disk.CreateNewDataSegmentInDirectory(s.DataDirectory)

	logger.SugaredLogger.Infof("Created new SSTable %s", ssTableFileName)

	kvPairs := memTable.GetKeyValPairs()

	f, deferFunc := disk.GetLogFile(s.DataDirectory+ssTableFileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	dataSegment.WriteMany(kvPairs, f)

	logger.SugaredLogger.Infof("Memtable %v written to SSTable %s", memTable, ssTableFileName)

	memTable.IsBeingWrittenToDisk = false
}
