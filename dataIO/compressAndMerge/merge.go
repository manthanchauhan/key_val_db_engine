package compressAndMerge

import (
	"bitcask/config/constants"
	"bitcask/dataIO/index/hashIndex"
	"bitcask/dataIO/index/hashIndex/dataSegment"
	"bitcask/dataIO/index/hashIndex/disk"
	"bitcask/logger"
	"bitcask/utils"
	"os"
)

func merge() {
	fileNames := getReadOnlySegmentFileNames()

	if len(fileNames) < 2 {
		return
	}

	for i := 0; i < len(fileNames); {
		start := i
		var end int

		var j int
		var mergedSegmentSize int64 = 0

		for j = i + 1; j <= len(fileNames) && mergedSegmentSize < constants.LogFileMaxSizeBytes; j++ {
			mergedSegmentSize += disk.GetSegmentFileSize(fileNames[j-1])
		}

		if mergedSegmentSize > constants.LogFileMaxSizeBytes {
			end = j - 2
		} else {
			end = j - 1
		}

		mergedSegmentFileName := mergeSegments(fileNames[start:end])
		hashIndex.GetHashIndex().ImportDataSegment(mergedSegmentFileName, hashMapImportSegmentInitValCheckForMerging(fileNames[start:end]))

		deleteSegments(fileNames[start:end])

		i = end
	}
}

func mergeSegments(fileNames []string) string {
	newFileName := disk.CreateNewDataSegment()

	f, deferFunc := disk.GetLogFile(utils.GetDataDirectory()+"/"+newFileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	execFunc := func(k string, v string, byteOffset int64) {
		dataSegment.Write(k, v, f)
	}

	for _, fileName := range fileNames {
		disk.ParseDataSegment(fileName, utils.GetDataDirectory(), execFunc)
	}

	return newFileName
}

func deleteSegments(fileNames []string) {
	for _, fileName := range fileNames {
		disk.DeleteSegment(fileName)
		logger.SugaredLogger.Infof("Deleting %s", fileName)
	}
}

func hashMapImportSegmentInitValCheckForMerging(mergedFileNames []string) func(k string) bool {
	return func(k string) bool {
		val, ok := hashIndex.GetHashIndex().GetDataLocation(k)

		if !ok {
			return false
		}

		initFileName, _ := disk.ExtractFileNameAndOffset(val)
		return utils.Contains(mergedFileNames, initFileName)
	}
}
