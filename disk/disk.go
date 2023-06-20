package disk

import (
	"bitcask/config/constants"
	"bitcask/disk/dataSegment"
	"bitcask/utils"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var LatestSegmentName = ""

//func Read(dataLocation string, dataDirectory string) string {
//scanner := GetDataLogScanner(dataLocation, dataDirectory)

//scanner.Scan()
//s := scanner.Text()
//return s
//}

func Write(key string, val string) string {
	logFileName := LatestSegmentName

	dataLocation := WriteInDataSegment(key, val, logFileName)

	if fileSize := GetSegmentFileSize(logFileName); fileSize >= constants.LogFileMaxSizeBytes {
		createNextDataSegment()
	}

	return dataLocation
}

func WriteInDataSegment(key string, val string, fileName string) string {
	f, deferFunc := GetLogFile(fileName, os.O_APPEND|os.O_WRONLY)
	defer deferFunc(f)

	byteCount := dataSegment.Write(key, val, f)

	fileSize := GetSegmentFileSize(fileName)

	byteOffset := fileSize - int64(byteCount)
	dataLocation := utils.GetDataLocationFromByteOffset(fileName, byteOffset)

	return dataLocation
}

func GetSegmentFileSize(fileName string) int64 {
	fileStat, err := os.Stat(utils.GetDataDirectory() + fileName)
	if err != nil {
		panic(err)
	}

	return fileStat.Size()
}

func DeleteSegment(fileName string) {
	if fileName == LatestSegmentName {
		panic("Deleting latest segment")
	}

	err := os.Remove(utils.GetDataDirectory() + fileName)
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

func FindLatestSegmentFileName() {
	dataSegmentFileNames := GetDataSegmentFileNameList(utils.GetDataDirectory())
	createdAtMax := time.Time{}
	latestSegmentFileName := ""

	for _, fileName := range dataSegmentFileNames {
		createdAt := GetCreatedAtFromSegmentFileName(fileName)

		if createdAt.After(createdAtMax) {
			createdAtMax = createdAt
			latestSegmentFileName = fileName
		}
	}

	if latestSegmentFileName == "" {
		createNextDataSegment()
	} else {
		SetLatestSegmentFileName(latestSegmentFileName)
	}
}

func SetLatestSegmentFileName(fileName string) {
	LatestSegmentName = fileName
}

func CreateNewDataSegmentInDirectory(dataDirectory string) string {
	fileName := fmt.Sprintf(constants.LogFileNameFormat, strconv.FormatInt(time.Now().UnixNano(), 10))

	file, err := os.Create(dataDirectory + fileName)
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

func CreateNewDataSegment() string {
	return CreateNewDataSegmentInDirectory(utils.GetDataDirectory())
}

func createNextDataSegment() {
	latestSegmentFileName := CreateNewDataSegment()
	SetLatestSegmentFileName(latestSegmentFileName)
}

func GetDataSegmentFileNameList(dataDirectory string) []string {
	entries, err := os.ReadDir(dataDirectory)
	if err != nil {
		panic(err)
	}

	var fileNames []string

	for _, entry := range entries {
		fileNames = append(fileNames, entry.Name())
	}

	return fileNames
}

func GetCreatedAtFromSegmentFileName(fileName string) time.Time {
	f, deferFunc := GetLogFile(utils.GetDataDirectory()+fileName, os.O_RDONLY)

	defer deferFunc(f)

	return dataSegment.GetCreatedAtFromSegmentFile(f)
}

func ParseDataSegment(fileName string, directory string, exec func(k string, v string, byteOffset int64)) {
	f, deferFunc := GetLogFile(directory+fileName, os.O_RDONLY)
	defer deferFunc(f)

	dataSegment.ParseDataSegment(f, exec)
}
