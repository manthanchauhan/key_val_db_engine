package disk

import (
	"bitcask/config/constants"
	"bitcask/dataIO/index/hashIndex/dataSegment"
	"bitcask/utils"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"
)

var singletonHashIndexDiskManager *HashIndexDiskManager

func GetHashIndexDiskManager() *HashIndexDiskManager {
	if singletonHashIndexDiskManager != nil {
		return singletonHashIndexDiskManager
	}

	singletonHashIndexDiskManager = &HashIndexDiskManager{
		DataDirectoryPath:   utils.GetDataDirectoryForIndex(constants.IndexTypeHashIndex),
		dataSegmentFileSize: constants.LogFileMaxSizeBytes,
	}

	singletonHashIndexDiskManager.Init()

	return singletonHashIndexDiskManager
}

type HashIndexDiskManager struct {
	DataDirectoryPath   string
	dataSegmentFileSize int64
	latestSegmentName   string
}

func (h *HashIndexDiskManager) Write(key string, val string) string {
	if h.latestSegmentName == "" {
		h.createNextDataSegment()
	}

	logFileName := h.latestSegmentName

	dataLocation := h.writeInDataSegment(key, val, logFileName)

	if fileSize := h.getSegmentFileSize(logFileName); fileSize >= h.dataSegmentFileSize {
		h.createNextDataSegment()
	}

	return dataLocation
}

func (h *HashIndexDiskManager) Delete(key string) string {
	return h.Write(key, constants.DeletedValuePlaceholder)
}

func (h *HashIndexDiskManager) GetLogFile(fileName string, flag int) (*os.File, func(file *os.File)) {
	f, err := os.OpenFile(h.DataDirectoryPath+"/"+fileName, flag, 0600)
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

func (h *HashIndexDiskManager) writeInDataSegment(key string, val string, fileName string) string {
	f, deferFunc := h.GetLogFile(fileName, os.O_APPEND|os.O_WRONLY)
	defer deferFunc(f)

	dataSegmentObj := dataSegment.DataSegment{Fdr: f}
	byteCount := dataSegmentObj.Write(key, val)

	fileSize := h.getSegmentFileSize(fileName)

	byteOffset := fileSize - int64(byteCount)
	return h.getDataLocationFromByteOffset(fileName, byteOffset)
}

func (h *HashIndexDiskManager) getSegmentFileSize(fileName string) int64 {
	fileStat, err := os.Stat(h.DataDirectoryPath + "/" + fileName)

	if err != nil {
		panic(err)
	}

	return fileStat.Size()
}

func (h *HashIndexDiskManager) getDataLocationFromByteOffset(segmentFileName string, byteOffset int64) string {
	return fmt.Sprintf("%s:%s", segmentFileName, strconv.FormatInt(byteOffset, 10))
}

func (h *HashIndexDiskManager) createNextDataSegment() {
	dataSegmentFactory := dataSegment.GetDataSegmentFactory()
	latestSegmentFileName := dataSegmentFactory.CreateDataSegment()
	h.latestSegmentName = latestSegmentFileName
}

func (h *HashIndexDiskManager) parseDataSegment(fileName string, exec func(k string, v string, byteOffset int64)) {
	f, deferFunc := h.GetLogFile(fileName, os.O_RDONLY)
	defer deferFunc(f)

	dataSegmentObj := dataSegment.DataSegment{Fdr: f}
	dataSegmentObj.Parse(exec)
}

func (h *HashIndexDiskManager) GetDataSegmentFileNameList() []string {
	entries, err := os.ReadDir(h.DataDirectoryPath)

	if err != nil {
		panic(err)
	}

	var fileNames []string

	for _, entry := range entries {
		fileNames = append(fileNames, entry.Name())
	}

	return fileNames
}

func (h *HashIndexDiskManager) setLatestSegmentFileName() {
	dataSegmentFileNames := h.GetDataSegmentFileNameList()
	createdAtMax := time.Time{}
	latestSegmentFileName := ""

	for _, fileName := range dataSegmentFileNames {
		createdAt := h.GetCreatedAtFromSegmentFileName(fileName)

		if createdAt.After(createdAtMax) {
			createdAtMax = createdAt
			latestSegmentFileName = fileName
		}
	}

	if latestSegmentFileName == "" {
		h.createNextDataSegment()
	} else {
		h.latestSegmentName = latestSegmentFileName
	}
}

func (h *HashIndexDiskManager) GetCreatedAtFromSegmentFileName(fileName string) time.Time {
	f, deferFunc := h.GetLogFile(fileName, os.O_RDONLY)

	defer deferFunc(f)

	return (&dataSegment.DataSegment{Fdr: f}).GetSegmentFileCreatedAt()
}

func (h *HashIndexDiskManager) Init() {
	h.setLatestSegmentFileName()
}

func (h *HashIndexDiskManager) ParseDataSegment(fileName string, exec func(k string, v string, byteOffset int64)) {
	f, deferFunc := h.GetLogFile(fileName, os.O_RDONLY)
	defer deferFunc(f)

	(&dataSegment.DataSegment{Fdr: f}).Parse(exec)
}
