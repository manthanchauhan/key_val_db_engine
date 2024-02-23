package hashIndex

import (
	"bitcask/config/constants"
	"bitcask/dataIO/index/hashIndex/dataSegment"
	"bitcask/dataIO/index/hashIndex/disk"
	"bitcask/utils"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
)

var singletonHashIndex *HashIndex

func GetHashIndex() *HashIndex {
	if singletonHashIndex != nil {
		return singletonHashIndex
	}

	singletonHashIndex = &HashIndex{
		hashMap:      &map[string]string{},
		hashMapMutex: &sync.RWMutex{},
		diskManager:  disk.GetHashIndexDiskManager(),
	}

	singletonHashIndex.Init()

	return singletonHashIndex
}

type HashIndex struct {
	hashMap      *map[string]string
	hashMapMutex *sync.RWMutex

	diskManager *disk.HashIndexDiskManager
}

func (h *HashIndex) Get(key string) (string, error) {
	dataLocation, isFound := h.GetDataLocation(key)

	if !isFound {
		return "", errors.New(constants.ErrMsgNotFound)
	}

	return h.getValue(dataLocation), nil
}

func (h *HashIndex) Set(key string, val string) error {
	defer h.useMutex()()

	dataLocation := h.diskManager.Write(key, val) // todo performance? handle clean mutex
	(*h.hashMap)[key] = dataLocation

	return nil
}

func (h *HashIndex) Delete(key string) error {
	defer h.useMutex()()

	h.diskManager.Delete(key) // todo performance?
	delete(*h.hashMap, key)

	return nil
}

func (h *HashIndex) Init() {
	h.ImportData()
}

func (h *HashIndex) ImportData() {
	dataSegmentList := h.diskManager.GetDataSegmentFileNameList()

	for _, fileName := range dataSegmentList {
		h.ImportDataSegment(fileName, nil)
	}
}

func (h *HashIndex) GetDataDirectory() string {
	return h.diskManager.DataDirectoryPath
}

func (h *HashIndex) GetDataLocation(key string) (string, bool) {
	defer h.useMutex()()
	val, ok := (*h.hashMap)[key]

	return val, ok
}

func (h *HashIndex) extractFileNameAndOffset(dataLocation string) (string, int64) {
	coords := strings.Split(dataLocation, ":")
	fileName := coords[0]

	byteOffset, err := strconv.Atoi(coords[1])
	if err != nil {
		panic(err)
	}

	return fileName, int64(byteOffset)
}

func (h *HashIndex) getValue(dataLocation string) string {
	fileName, offset := h.extractFileNameAndOffset(dataLocation)

	f, deferFunc := h.diskManager.GetLogFile(fileName, os.O_RDONLY)
	defer deferFunc(f)

	dataSegmentObj := dataSegment.DataSegment{Fdr: f}

	return dataSegmentObj.ReadAtOffset(&offset).Val
}

func (h *HashIndex) ImportDataSegment(fileName string, initValCheck func(k string) bool) {
	h.diskManager.ParseDataSegment(fileName, func(k string, v string, byteOffset int64) {
		if initValCheck == nil || initValCheck(k) {
			dataLocation := utils.GetDataLocationFromByteOffset(fileName, byteOffset)

			if utils.EqualsIgnoreCase(v, constants.DeletedValuePlaceholder) {
				return
			}

			h.hashMapMutex.Lock()
			(*h.hashMap)[k] = dataLocation
			h.hashMapMutex.Unlock()
		}
	})
}

func (h *HashIndex) useMutex() func() {
	h.hashMapMutex.Lock()

	return func() {
		h.hashMapMutex.Unlock()
	}
}
