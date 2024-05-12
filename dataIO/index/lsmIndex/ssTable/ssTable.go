package ssTable

import (
	"bitcask/config/constants"
	"bitcask/dataIO/dataSegment"
	"bitcask/dataIO/index/hashIndex/disk"
	"bitcask/dataIO/index/lsmIndex/memTable"
	"bitcask/logger"
	"os"
)

type SSTable struct {
	hashMap        *map[string]*int64
	orderedKeyList *[]string
	FileName       string
	Directory      string
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
	logger.SugaredLogger.Infof("Ordered Key List %v", *s.orderedKeyList)
	blockKey := findJustLessThanEqualString(*s.orderedKeyList, key)
	return (*s.hashMap)[blockKey], nil
}

func findJustLessThanEqualString(orderedList []string, key string) string {
	low := 0
	high := len(orderedList) - 1

	for low <= high {
		mid := (high + low) / 2

		if orderedList[mid] == key {
			return orderedList[mid]
		}

		if orderedList[mid] < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if high < 0 {
		return ""
	}

	return orderedList[high]
}

func (s *SSTable) getValueFromBlock(key string, blockOffset int64) (*string, error) {
	f, deferFunc := dataSegment.GetLogFile(s.Directory+"/"+s.FileName, os.O_RDONLY)
	defer deferFunc(f)

	scanner := dataSegment.GetDataLogScanner(f, &blockOffset)

	i := 0

	for scanner.Scan() {
		if i >= constants.SSTableBlockMaxKeys {
			break
		}

		dataLine := scanner.Text()

		if k, v := dataSegment.ExtractKeyVal(dataLine); k == key {
			return &v, nil
		}

		i++
	}

	return nil, nil
}

func (s *SSTable) buildIndex() {
	i := 0

	s.hashMap = &map[string]*int64{}
	s.orderedKeyList = &[]string{}

	disk.ParseDataSegment(s.FileName, s.Directory, func(k string, v string, byteOffset int64) {
		if i%constants.SSTableBlockMaxKeys == 0 {
			(*s.hashMap)[k] = &byteOffset
			*s.orderedKeyList = append(*s.orderedKeyList, k)
		}
		i++
	})

	logger.SugaredLogger.Infof("Ordered key list %v", *s.orderedKeyList)
}

func NewSSTableFromMemTable(memTable *memTable.MemTable, directory string) (*SSTable, error) {
	ssTable := SSTable{
		Directory: directory,
	}

	ssTable.FileName = disk.CreateNewDataSegmentInDirectory(directory)

	logger.SugaredLogger.Infof("Created new SSTable %s", ssTable.FileName)

	kvPairs := memTable.GetKeyValPairs()

	f, deferFunc := dataSegment.GetLogFile(ssTable.Directory+"/"+ssTable.FileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	dataSegment.WriteMany(kvPairs, f)

	ssTable.buildIndex()

	logger.SugaredLogger.Infof("Memtable %v written to SSTable %s", memTable, ssTable.FileName)

	return &ssTable, nil
}

func NewSSTableFromFileName(fileName string, directory string) *SSTable {
	ssTable := SSTable{
		Directory: directory,
	}

	ssTable.FileName = fileName
	ssTable.buildIndex()

	return &ssTable
}
