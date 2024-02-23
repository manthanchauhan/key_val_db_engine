package compressAndMerge

import (
	"bitcask/dataIO/index/hashIndex/disk"
	"bitcask/utils"
	"io/fs"
	"syscall"
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
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(*fs.PathError); ok && err.Err == syscall.ENOENT {
				return
			} else {
				panic(r)
			}
		}
	}()

	compress()
	merge()
}

func getReadOnlySegmentFileNames() []string {
	segmentFileNames := disk.GetDataSegmentFileNameList(utils.GetDataDirectory())
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
