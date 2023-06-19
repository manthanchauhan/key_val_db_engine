package ssTable

import (
	"bitcask/config/constants"
	"bitcask/disk"
	"bitcask/disk/dataSegment"
	"bitcask/logger"
	"bitcask/lsmIndex/memTable"
	"fmt"
	orderedMap "github.com/wk8/go-ordered-map"
	"os"
	"strconv"
)

type SSTable struct {
	hashMap   *orderedMap.OrderedMap
	fileName  string
	directory string
}

func (s *SSTable) Get(key string) (string, bool) {
	blockOffset, err := s.getBlockOffset(key)
	if err != nil {
		panic(err)
	}

	if blockOffset == nil {
		return "", false
	}

	val, err := s.getValueFromBlock(key, *blockOffset)
	if err != nil {
		panic(err)
	}

	if val == nil {
		return "", false
	} else {
		return *val, true
	}
}

func (s *SSTable) getBlockOffset(key string) (*int64, error) {
	var blockOffset *int64 = nil

	for pair := s.hashMap.Oldest(); pair != nil; pair = pair.Next() {
		k := pair.Key.(string)

		if key >= k {
			v := pair.Value.(int64)
			blockOffset = &v
		}
	}

	return blockOffset, nil
}

func (s *SSTable) getValueFromBlock(key string, blockOffset int64) (*string, error) {
	dataLocation := fmt.Sprintf("%s:%s", s.fileName, strconv.FormatInt(blockOffset, 10))

	scanner := disk.GetDataLogScanner(dataLocation, s.directory)

	for i := 0; i < 10; i++ {
		data := scanner.Text()
		if k, v := dataSegment.ExtractKeyVal(data); k == key {
			return &v, nil
		}

		scanner.Scan()
	}

	return nil, nil
}

func (s *SSTable) buildIndex() {
	i := 0

	s.hashMap = orderedMap.New()

	disk.ParseDataSegment(s.fileName, s.directory, func(k string, v string, byteOffset int64) {
		if i%constants.SSTableBlockMaxKeys == 0 {
			s.hashMap.Set(k, byteOffset)
		}

		i++
	})
}

func NewSSTable(memTable *memTable.MemTable, directory string) (*SSTable, error) {
	ssTable := SSTable{
		directory: directory,
	}

	ssTable.fileName = disk.CreateNewDataSegmentInDirectory(directory)

	logger.SugaredLogger.Infof("Created new SSTable %s", ssTable.fileName)

	kvPairs := memTable.GetKeyValPairs()

	f, deferFunc := disk.GetLogFile(ssTable.directory+ssTable.fileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	dataSegment.WriteMany(kvPairs, f)

	ssTable.buildIndex()

	logger.SugaredLogger.Infof("Memtable %v written to SSTable %s", memTable, ssTable.fileName)

	return &ssTable, nil
}
