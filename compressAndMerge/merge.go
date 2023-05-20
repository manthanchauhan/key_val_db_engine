package compressAndMerge

import (
	"bitcask/config/constants"
	"bitcask/disk"
	"bitcask/disk/dataSegment"
	"bitcask/hashIndex"
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
		hashIndex.ImportDataSegment(mergedSegmentFileName)

		deleteSegments(fileNames[start:end])

		i = end
	}
}

func mergeSegments(fileNames []string) string {
	newFileName := disk.CreateNewDataSegment()

	f, deferFunc := disk.GetLogFile(newFileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	execFunc := func(k string, v string, byteOffset int64) {
		dataSegment.Write(k, v, f)
	}

	for _, fileName := range fileNames {
		disk.ParseDataSegment(fileName, execFunc)
	}

	return newFileName
}

func deleteSegments(fileNames []string) {
	for _, fileName := range fileNames {
		disk.DeleteSegment(fileName)
	}
}
