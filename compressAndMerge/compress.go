package compressAndMerge

import (
	"bitcask/config/constants"
	"bitcask/disk"
	"bitcask/disk/dataSegment"
	"bitcask/hashIndex"
	"bitcask/utils"
	"os"
)

func compress() {
	fileNames := getReadOnlySegmentFileNames()

	for _, fileName := range fileNames {
		compressSegment(fileName)
	}
}

func compressSegment(fileName string) {
	newFileName := createCompressedSegment(fileName)

	isCompressed := disk.GetSegmentFileSize(fileName) > disk.GetSegmentFileSize(newFileName)
	newFileIsEmpty := disk.GetSegmentFileSize(newFileName) == constants.DataSegmentMetaDataByteSize

	if isCompressed {
		if newFileIsEmpty {
			disk.DeleteSegment(newFileName)
		} else {
			hashIndex.ImportDataSegment(newFileName, hashMapImportSegmentInitValCheckForCompression(fileName))
		}

		disk.DeleteSegment(fileName)
	} else {
		disk.DeleteSegment(newFileName)
	}
}

func createCompressedSegment(originalSegmentFileName string) string {
	newFileName := disk.CreateNewDataSegment()

	f, deferFunc := disk.GetLogFile(utils.GetDataDirectory()+newFileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	disk.ParseDataSegment(originalSegmentFileName, utils.GetDataDirectory(), func(k string, v string, byteOffset int64) {
		dataLocation := utils.GetDataLocationFromByteOffset(originalSegmentFileName, byteOffset)

		isUsed := dataLocation == hashIndex.GetDataLocationOrPanic(k)

		if isUsed {
			dataSegment.Write(k, v, f)
		}
	})

	return newFileName
}

func hashMapImportSegmentInitValCheckForCompression(compressedFileName string) func(k string) bool {
	return func(k string) bool {
		val, ok := hashIndex.GetDataLocation(k)

		if !ok {
			return false
		}

		initFileName, _ := disk.ExtractFileNameAndOffset(val)
		return initFileName == compressedFileName
	}
}
