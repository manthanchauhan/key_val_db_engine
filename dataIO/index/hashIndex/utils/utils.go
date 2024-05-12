package utils

import (
	"bitcask/config/constants"
	"bitcask/dataIO/dataSegment"
	"bitcask/dataIO/index/hashIndex/disk"
	"bitcask/utils"
)

func GetReadOnlySegmentFileNames() []string {
	segmentFileNames := dataSegment.GetDataSegmentFileNameList(utils.GetDataDirectoryForIndex(constants.IndexTypeHashIndex))
	readOnlySegmentFileNames := make([]string, len(segmentFileNames))
	i := 0

	for _, fileName := range segmentFileNames {
		if fileName != disk.LatestSegmentName {
			readOnlySegmentFileNames[i] = fileName
			i += 1
		}
	}

	return readOnlySegmentFileNames[:i]
}
