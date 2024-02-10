package dataIO

import (
	"bitcask/config/constants"
	"bitcask/disk"
	"bitcask/hashIndex"
	"strings"
)

func Read(key string) string {
	dataLocation := hashIndex.GetDataLocation(key)
	block := disk.Read(dataLocation)
	val := strings.Split(block, constants.LogKeyValDelim)[1]

	if strings.ToUpper(strings.TrimSpace(val)) == constants.DeletedValuePlaceholder {
		panic(constants.NotFoundMsg)
	}

	return val
}

func Write(key string, val string) {
	dataLocation := disk.Write(key, val)
	hashIndex.Set(key, dataLocation)
}

func Delete(key string) {
	disk.Delete(key)
	hashIndex.Delete(key)
}
