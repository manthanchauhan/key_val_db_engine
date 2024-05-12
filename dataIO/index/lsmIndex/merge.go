package lsmIndex

import (
	"bitcask/config/constants"
	dataSegment2 "bitcask/dataIO/dataSegment"
	"bitcask/dataIO/index/hashIndex/disk"
	"bitcask/utils"
	"cmp"
	"os"
	"slices"
	"strconv"
	"strings"
)

func Merge() {
	dataDirectory := utils.GetDataDirectoryForIndex(constants.IndexTypeLSMIndex)

	dataSegmentFileNameList := dataSegment2.GetDataSegmentFileNameList(dataDirectory)

	if len(dataSegmentFileNameList) < 2 {
		return
	}

	groupSSTablesAndMerge(dataSegmentFileNameList)
}

func groupSSTablesAndMerge(fileNames []string) {
	dataDirectory := utils.GetDataDirectoryForIndex(constants.IndexTypeLSMIndex)

	sortFileNamesByCreationTime(fileNames)

	for i := 0; i < len(fileNames); {
		start := i
		var end int

		var j int
		var mergedSegmentSize int64 = 0

		for j = i + 1; j <= len(fileNames) && mergedSegmentSize < constants.MemTableMaxSizeBytes; j++ {
			mergedSegmentSize += disk.GetSegmentFileSize(fileNames[j-1], dataDirectory)
		}

		if mergedSegmentSize > constants.LogFileMaxSizeBytes {
			end = j - 2
		} else {
			end = j - 1
		}

		mergedSegmentFileName := createMergedSegment(fileNames[start:end])

		GetLsmIndex().ImportDataSegment(mergedSegmentFileName)

		deleteSegments(fileNames[start:end])

		i = end
	}
}

func mergeSegments(fileNames []string) []dataSegment2.Record {
	if len(fileNames) == 1 {
		return dataSegment2.ReadAllRecordsFromDataSegment(fileNames[0])
	}

	leftFileRecords := mergeSegments(fileNames[:len(fileNames)/2])
	rightFileRecords := mergeSegments(fileNames[len(fileNames)/2:])

	var mergedRecords []dataSegment2.Record

	leftI := 0
	rightI := 0

	for leftI < len(leftFileRecords) && rightI < len(rightFileRecords) {
		if leftFileRecords[leftI].Key < rightFileRecords[rightI].Key {
			mergedRecords = append(mergedRecords, leftFileRecords[leftI])
			leftI++
		} else {
			mergedRecords = append(mergedRecords, rightFileRecords[rightI])
			rightI++
		}
	}

	for leftI < len(leftFileRecords) {
		mergedRecords = append(mergedRecords, leftFileRecords[leftI])
		leftI++
	}

	for rightI < len(rightFileRecords) {
		mergedRecords = append(mergedRecords, rightFileRecords[rightI])
		rightI++
	}

	return mergedRecords
}

func createMergedSegment(fileNames []string) string {
	mergedSegmentRecords := mergeSegments(fileNames)

	dataDirectory := utils.GetDataDirectoryForIndex(constants.IndexTypeLSMIndex)
	newFileName := disk.CreateNewDataSegmentInDirectory(dataDirectory)

	f, deferFunc := dataSegment2.GetLogFile(dataDirectory+"/"+newFileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	for _, record := range mergedSegmentRecords {
		dataSegment2.Write(record.Key, record.Val, f)
	}

	return newFileName
}

func sortFileNamesByCreationTime(fileNames []string) {
	epochComparator := func(a, b string) int {
		aWithoutExt := a[:len(a)-len(".log")]
		bWithoutExt := b[:len(b)-len(".log")]

		aEpoch, err := strconv.Atoi(strings.Split(aWithoutExt, "_")[1])
		if err != nil {
			panic(err)
		}

		bEpoch, err := strconv.Atoi(strings.Split(bWithoutExt, "_")[1])
		if err != nil {
			panic(err)
		}

		return cmp.Compare(aEpoch, bEpoch)
	}

	slices.SortFunc(fileNames, epochComparator)
}

func deleteSegments(fileNames []string) {
	GetLsmIndex().RemoveSSTables(fileNames)

	for _, fileName := range fileNames {
		disk.DeleteSegment(fileName, utils.GetDataDirectoryForIndex(constants.IndexTypeLSMIndex))
	}
}
