package hashIndex

import (
	"bitcask/config/constants"
	"bitcask/disk"
	"bitcask/utils"
)

var hashMap = map[string]string{}

func GetDataLocation(key string) string {
	dataLocation, ok := hashMap[key]

	if ok != true {
		panic(constants.NotFoundMsg)
	}

	return dataLocation
}

func SetByteOffset(key string, offset string) {
	hashMap[key] = offset
}

func Build() {
	hashMap = map[string]string{}

	dataSegmentFileNames := disk.GetDataSegmentFileNameList()

	for _, fileName := range dataSegmentFileNames {
		ImportDataSegment(fileName)
	}

}

func ImportDataSegment(fileName string) {
	disk.ParseDataSegment(fileName, func(k string, v string, byteOffset int64) {
		dataLocation := utils.GetDataLocationFromByteOffset(fileName, byteOffset)
		SetByteOffset(k, dataLocation)
	})
}
