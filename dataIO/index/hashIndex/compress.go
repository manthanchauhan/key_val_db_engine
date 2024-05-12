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

func Compress() {
	fileNames := utils2.GetReadOnlySegmentFileNames()

	for _, fileName := range fileNames {
		compressSegment(fileName)
	}
}

func compressSegment(fileName string) {
	dataDirectory := utils.GetDataDirectoryForIndex(constants.IndexTypeHashIndex)
	newFileName := createCompressedSegment(fileName)

	isCompressed := disk.GetSegmentFileSize(fileName, dataDirectory) > disk.GetSegmentFileSize(newFileName, dataDirectory)
	newFileIsEmpty := disk.GetSegmentFileSize(newFileName, dataDirectory) == constants.DataSegmentMetaDataByteSize

	if isCompressed {
		if newFileIsEmpty {
			disk.DeleteSegment(newFileName, dataDirectory)
		} else {
			GetHashIndex().ImportDataSegment(newFileName, hashMapImportSegmentInitValCheckForCompression(fileName))
		}

		disk.DeleteSegment(fileName, dataDirectory)
		logger.SugaredLogger.Infof("Deleting %s/%s", dataDirectory, fileName)
	} else {
		disk.DeleteSegment(newFileName, dataDirectory)
	}
}

func createCompressedSegment(originalSegmentFileName string) string {
	newFileName := disk.CreateNewDataSegmentInDirectory(utils.GetDataDirectoryForIndex(constants.IndexTypeHashIndex))

	f, deferFunc := dataSegment.GetLogFile(utils.GetDataDirectory()+"/"+newFileName, os.O_WRONLY|os.O_APPEND)
	defer deferFunc(f)

	disk.ParseDataSegment(originalSegmentFileName, utils.GetDataDirectory(), func(k string, v string, byteOffset int64) {
		dataLocation := utils.GetDataLocationFromByteOffset(originalSegmentFileName, byteOffset)

		if !utils.EqualsIgnoreCase(v, constants.DeletedValuePlaceholder) {
			hashedDataLocation, isFound := GetHashIndex().GetDataLocation(k)

			if isFound && hashedDataLocation == dataLocation {
				dataSegment.Write(k, v, f)
			}
		}
	})

	return newFileName
}

func hashMapImportSegmentInitValCheckForCompression(compressedFileName string) func(k string) bool {
	return func(k string) bool {
		val, ok := GetHashIndex().GetDataLocation(k)

		if !ok {
			return false
		}

		initFileName, _ := disk.ExtractFileNameAndOffset(val)
		return initFileName == compressedFileName
	}
}
