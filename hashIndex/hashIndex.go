package hashIndex

import (
	"bitcask/config/constants"
	"bitcask/disk"
	"bitcask/utils"
	"sync"
)

//type HashIndex struct {
//	hashMap *map[string]string{}
//}

var hashMap = map[string]string{}
var hashMapMutex = &sync.RWMutex{}

func GetDataLocationOrPanic(key string) string {
	dataLocation, ok := GetDataLocation(key)

	if ok != true {
		panic(constants.NotFoundMsg)
	}

	return dataLocation
}

func GetDataLocation(key string) (string, bool) {
	hashMapMutex.Lock()
	val, ok := hashMap[key]
	hashMapMutex.Unlock()

	return val, ok
}

func Set(key string, val string) {
	hashMapMutex.Lock()
	hashMap[key] = val
	hashMapMutex.Unlock()
}

func Build() {
	hashMap = map[string]string{}

	disk.FindLatestSegmentFileName()

	dataSegmentFileNames := disk.GetDataSegmentFileNameList()

	for _, fileName := range dataSegmentFileNames {
		ImportDataSegment(fileName, nil)
	}
}

func ImportDataSegment(fileName string, initValCheck func(k string) bool) {
	disk.ParseDataSegment(fileName, func(k string, v string, byteOffset int64) {
		if initValCheck == nil || initValCheck(k) {
			dataLocation := utils.GetDataLocationFromByteOffset(fileName, byteOffset)
			Set(k, dataLocation)
		}
	})
}
