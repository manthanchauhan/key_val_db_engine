package dataIO

import (
	"bitcask/config/constants"
	"bitcask/index"
	"bitcask/logger"
	"bitcask/lsmIndex"
	"bitcask/utils"
	"fmt"
)

var Index index.Index = nil

func Read(key string) string {
	logger.SugaredLogger.Info("Reading key - ", key)

	val := Index.GetOrPanic(key)

	logger.SugaredLogger.Info("Found val - ", val)
	return val
}

func Write(key string, val string) {
	logger.SugaredLogger.Infof("Writing key - %s, val - %s", key, val)

	if err := Index.Set(key, val); err != nil {
		panic(err)
	}

	logger.SugaredLogger.Info("Written")
}

func Init() {
	var err error

	indexType := utils.GetIndexType()

	switch indexType {
	//
	//case constants.IndexTypeHashIndex:
	//	hashIndex.Init()
	//	go compressAndMerge.CompressionAndMergingGoRoutine() todo

	case constants.IndexTypeLSMIndex:
		Index, err = lsmIndex.NewLsmIndex()

	default:
		panic(fmt.Sprintf("Invalid index type - %s", indexType))
	}

	logger.SugaredLogger.Info("Using Index Type - ", indexType)

	if err != nil {
		panic(err)
	}
}
