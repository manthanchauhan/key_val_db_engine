package disk

import (
	"bitcask/config/constants"
	"bitcask/dataIO/dataSegment"
	"bitcask/logger"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var LatestSegmentName = ""

func GetSegmentFileSize(fileName string, directory string) int64 {
	fileStat, err := os.Stat(directory + "/" + fileName)
	if err != nil {
		panic(err)
	}

	return fileStat.Size()
}

func DeleteSegment(fileName string, directory string) {
	if fileName == LatestSegmentName {
		panic("Deleting latest segment")
	}

	logger.SugaredLogger.Infof("Removing file %s after compress & merge", fileName)

	err := os.Remove(directory + "/" + fileName)
	if err != nil {
		panic(err)
	}
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

	segmentMetaData := dataSegment.MetaDataDto{CreatedAt: time.Now()}
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

func ParseDataSegment(fileName string, directory string, exec func(k string, v string, byteOffset int64)) {
	f, deferFunc := dataSegment.GetLogFile(directory+"/"+fileName, os.O_RDONLY)
	defer deferFunc(f)

	dataSegment.ParseDataSegment(f, exec)
}
