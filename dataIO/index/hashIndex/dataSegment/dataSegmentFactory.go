package dataSegment

import (
	"bitcask/config/constants"
	"bitcask/utils"
	"fmt"
	"os"
	"strconv"
	"time"
)

var singletonDataSegmentFactory *Factory

func GetDataSegmentFactory() *Factory {
	if singletonDataSegmentFactory != nil {
		return singletonDataSegmentFactory
	}

	singletonDataSegmentFactory = &Factory{
		fileNameFormat:    constants.LogFileNameFormat,
		dataDirectoryPath: utils.GetDataDirectoryForIndex(constants.IndexTypeHashIndex),
	}
	return singletonDataSegmentFactory
}

type Factory struct {
	fileNameFormat    string
	dataDirectoryPath string
}

func (b *Factory) CreateDataSegment() string {
	fileName := fmt.Sprintf(b.fileNameFormat, strconv.FormatInt(time.Now().UnixNano(), 10))

	file, err := os.Create(b.dataDirectoryPath + fileName)
	if err != nil {
		panic(err)
	}

	segmentMetaData := MetaDataDto{CreatedAt: time.Now()}
	byteArr := segmentMetaData.ToByteArr()

	if _, err := file.Write(byteArr); err != nil {
		panic(err)
	}

	err = file.Close()
	if err != nil {
		panic(err)
	}

	return fileName
}
