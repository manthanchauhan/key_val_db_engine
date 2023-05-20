package compressAndMerge

import (
	"bitcask/disk"
	"time"
)

func CompressionAndMergingGoRoutine() {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			compressAndMerge()
		}
	}
}

func compressAndMerge() {
	compress()
	merge()
}

func getReadOnlySegmentFileNames() []string {
	segmentFileNames := disk.GetDataSegmentFileNameList()
	readOnlySegmentFileNames := make([]string, len(segmentFileNames)-1)
	i := 0

	for _, fileName := range segmentFileNames {
		if fileName != disk.LatestSegmentName {
			readOnlySegmentFileNames[i] = fileName
			i += 1
		}
	}

	return readOnlySegmentFileNames
}
