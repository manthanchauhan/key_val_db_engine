package dataSegment

import (
	"bitcask/config/constants"
	"bitcask/utils"
	"bufio"
	"bytes"
	"os"
	"strings"
	"time"
)

func getMetaDataFromSegmentFileName(f *os.File) []byte {
	byteArr := make([]byte, constants.DataSegmentMetaDataByteSize)

	_, err := f.Read(byteArr)
	if err != nil {
		panic(err)
	}

	return byteArr
}

func GetCreatedAtFromSegmentFile(f *os.File) time.Time {
	segmentMetaDataJson := getMetaDataFromSegmentFileName(f)

	segmentMetaData := MetaDataDto{}
	segmentMetaData.FromByteArr(segmentMetaDataJson)

	return segmentMetaData.CreatedAt
}

func GetDataLogScanner(f *os.File) *bufio.Scanner {
	if _, err := f.Seek(constants.DataSegmentMetaDataByteSize, 0); err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(f)
	scanner.Split(SplitAt(constants.LogNewLineDelim))
	return scanner
}

func SplitAt(substring string) func(data []byte, atEOF bool) (advance int, token []byte, err error) {
	searchBytes := []byte(substring)
	searchLen := len(searchBytes)
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		dataLen := len(data)

		// Return nothing if at end of file and no dataIO passed
		if atEOF && dataLen == 0 {
			return 0, nil, nil
		}

		// Find next separator and return token
		if i := bytes.Index(data, searchBytes); i >= 0 {
			return i + searchLen, data[0:i], nil
		}

		// If we're at EOF, we have a final, non-terminated line. Return it.
		if atEOF {
			return dataLen, data, nil
		}

		// Request more dataIO.
		return 0, nil, nil
	}
}

func ExtractKeyVal(dataLine string) (string, string) {
	keyVal := strings.Split(dataLine, constants.LogKeyValDelim)
	key := keyVal[0]
	val := keyVal[1]
	return key, val
}

func ParseDataSegment(f *os.File, exec func(k string, v string, byteOffset int64)) {
	scanner := GetDataLogScanner(f)

	var byteOffset int64 = constants.DataSegmentMetaDataByteSize

	for scanner.Scan() {
		dataLine := scanner.Text()
		key, val := ExtractKeyVal(dataLine)

		exec(key, val, byteOffset)

		byteOffset += utils.GetBlockSize(key, val)
	}
}

func Write(key string, val string, f *os.File) int {
	byteString := []byte(key + constants.LogKeyValDelim + val + constants.LogNewLineDelim)

	byteCount, err := f.Write(byteString)
	if err != nil {
		panic(err)
	}

	return byteCount
}
