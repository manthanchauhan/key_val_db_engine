package disk

import (
	"bitcask/config/constants"
	dataSegment2 "bitcask/dataIO/index/hashIndex/dataSegment"
	"bitcask/logger"
	"bitcask/utils"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var LatestSegmentName = ""

func GetSegmentFileSize(fileName string) int64 {
	fileStat, err := os.Stat(utils.GetDataDirectory() + "/" + fileName)
	if err != nil {
		panic(err)
	}

	return fileStat.Size()
}

func DeleteSegment(fileName string) {
	if fileName == LatestSegmentName {
		panic("Deleting latest segment")
	}

	logger.SugaredLogger.Infof("Removing file %s after compress & merge", fileName)

	err := os.Remove(utils.GetDataDirectory() + "/" + fileName)
	if err != nil {
		panic(err)
	}
}

func GetLogFile(fileName string, flag int) (*os.File, func(file *os.File)) {
	f, err := os.OpenFile(fileName, flag, 0600)
	if err != nil {
		panic(err)
	}

	deferFunc := func(f *os.File) {
		err := f.Close()
		if err != nil && err != syscall.EBADF {
			panic(err)
		}
	}

	return f, deferFunc
}

func ExtractFileNameAndOffset(dataLocation string) (string, int64) {
	coords := strings.Split(dataLocation, ":")
	fileName := coords[0]

	byteOffset, err := strconv.Atoi(coords[1])
	if err != nil {
		panic(err)
	}

	return fileName, int64(byteOffset)
}

func CreateNewDataSegmentInDirectory(dataDirectory string) string {
	fileName := fmt.Sprintf(constants.LogFileNameFormat, strconv.FormatInt(time.Now().UnixNano(), 10))

	file, err := os.Create(dataDirectory + "/" + fileName)
	if err != nil {
		panic(err)
	}

	segmentMetaData := dataSegment2.MetaDataDto{CreatedAt: time.Now()}
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

func CreateNewDataSegment() string {
	return CreateNewDataSegmentInDirectory(utils.GetDataDirectory())
}

func GetDataSegmentFileNameList(dataDirectory string) []string {
	entries, err := os.ReadDir(dataDirectory)
	if err != nil {
		panic(err)
	}

	var fileNames []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileNames = append(fileNames, entry.Name())
	}

	return fileNames
}

func ParseDataSegment(fileName string, directory string, exec func(k string, v string, byteOffset int64)) {
	f, deferFunc := GetLogFile(directory+"/"+fileName, os.O_RDONLY)
	defer deferFunc(f)

	dataSegment2.ParseDataSegment(f, exec)
}
