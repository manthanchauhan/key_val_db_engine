package hashIndex

import (
	"bitcask/config/constants"
	"bitcask/dataIO/dataSegment"
	"bitcask/dataIO/index/hashIndex/disk"
	utils2 "bitcask/dataIO/index/hashIndex/utils"
	"bitcask/logger"
	"bitcask/utils"
	"os"
)

func Merge() {
	fileNames := utils2.GetReadOnlySegmentFileNames()

	if len(fileNames) < 2 {
		return
	}

	for i := 0; i < len(fileNames); {
		start := i
		var end int

		var j int
		var mergedSegmentSize int64 = 0

		for j = i + 1; j <= len(fileNames) && mergedSegmentSize < constants.LogFileMaxSizeBytes; j++ {
			mergedSegmentSize += disk.GetSegmentFileSize(fileNames[j-1], utils.GetDataDirectoryForIndex(constants.IndexTypeHashIndex))
		}

		if mergedSegmentSize > constants.LogFileMaxSizeBytes {
			end = j - 2
		} else {
			end = j - 1
		}

		mergedSegmentFileName := mergeSegments(fileNames[start:end])
		GetHashIndex().ImportDataSegment(mergedSegmentFileName, hashMapImportSegmentInitValCheckForMerging(fileNames[start:end]))

		deleteSegments(fileNames[start:end])

		i = end
	}
}

func mergeSegments(fileNames []string) string {
	dataDirectory := utils.GetDataDirectoryForIndex(constants.IndexTypeHashIndex)

	newFileName := disk.CreateNewDataSegmentInDirectory(dataDirectory)

	f, deferFunc := dataSegment.GetLogFile(dataDirectory+"/"+newFileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	execFunc := func(k string, v string, byteOffset int64) {
		dataSegment.Write(k, v, f)
	}

	for _, fileName := range fileNames {
		disk.ParseDataSegment(fileName, dataDirectory, execFunc)
	}

	return newFileName
}

func deleteSegments(fileNames []string) {
	dataDirectory := utils.GetDataDirectoryForIndex(constants.IndexTypeHashIndex)

	for _, fileName := range fileNames {
		disk.DeleteSegment(fileName, dataDirectory)
		logger.SugaredLogger.Infof("Deleting %s/%s", dataDirectory, fileName)
	}
}

func hashMapImportSegmentInitValCheckForMerging(mergedFileNames []string) func(k string) bool {
	return func(k string) bool {
		val, ok := GetHashIndex().GetDataLocation(k)

		if !ok {
			return false
		}

		initFileName, _ := disk.ExtractFileNameAndOffset(val)
		return utils.Contains(mergedFileNames, initFileName)
	}
}
