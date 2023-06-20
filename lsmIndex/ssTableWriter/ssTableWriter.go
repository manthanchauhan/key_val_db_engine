package ssTableWriter

import (
	"bitcask/logger"
	"bitcask/lsmIndex/memTable"
	"bitcask/lsmIndex/ssTable"
	"fmt"
)

type SSTableWriter struct {
	SuccessChan    *chan *memTable.MemTable
	jobChan        chan *memTable.MemTable
	NewSSTableChan *chan *ssTable.SSTable

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

				ssTable_ := s.writeMemTableToSSTable(memTable_)

				*s.SuccessChan <- memTable_
				*s.NewSSTableChan <- ssTable_

				logger.SugaredLogger.Infof("MemTable %v queued for destruction", memTable_)
			}
		}
	}

	if !s.isWriteGoRoutineRunning {
		go s.writeGoRoutine()
	}

	s.isInitialized = true
}

func (s *SSTableWriter) writeMemTableToSSTable(memTable *memTable.MemTable) *ssTable.SSTable {
	memTable.IsBeingWrittenToDisk = true

	ssTable_, err := ssTable.NewSSTableFromMemTable(memTable, s.DataDirectory)

	if err != nil {
		panic(err)
	}

	memTable.IsWrittenToSSTable()

	return ssTable_
}
