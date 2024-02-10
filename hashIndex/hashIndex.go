package hashIndex

import (
	"bitcask/config/constants"
	"bitcask/disk"
	"bitcask/utils"
	"sync"
)

var hashMap = map[string]string{}
var hashMapMutex = &sync.RWMutex{}

func GetDataLocation(key string) string {
	dataLocation, ok := Get(key)

	if ok != true {
		panic(constants.NotFoundMsg)
	}

	return dataLocation
}

func Get(key string) (string, bool) {
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

func Delete(key string) {
	hashMapMutex.Lock()
	delete(hashMap, key)
	hashMapMutex.Unlock()
}

func Build() {
	hashMap = map[string]string{}

	dataSegmentFileNames := disk.GetDataSegmentFileNameList()

	for _, fileName := range dataSegmentFileNames {
		ImportDataSegment(fileName, nil)
	}

}

func ImportDataSegment(fileName string, initValCheck func(k string) bool) {
	disk.ParseDataSegment(fileName, func(k string, v string, byteOffset int64) {
		if initValCheck == nil || initValCheck(k) {
			dataLocation := utils.GetDataLocationFromByteOffset(fileName, byteOffset)

			if utils.EqualsIgnoreCase(v, constants.DeletedValuePlaceholder) {
				return
			}

			Set(k, dataLocation)
		}
	})
}
