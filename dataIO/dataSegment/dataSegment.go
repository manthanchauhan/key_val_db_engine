package dataSegment

import (
	"bitcask/config/constants"
	"bitcask/logger"
	"bitcask/utils"
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type DataSegment struct {
	Fdr *os.File
}

func (d *DataSegment) getDataLogScanner(offset *int64) *bufio.Scanner {
	var offset_ int64

	if offset == nil {
		offset_ = constants.DataSegmentMetaDataByteSize
	} else {
		offset_ = *offset
	}

	if _, err := d.Fdr.Seek(offset_, 0); err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(d.Fdr)
	scanner.Split(SplitAt(constants.LogNewLineDelim))
	return scanner
}

func (d *DataSegment) ReadAtOffset(offset *int64) *Record {
	scanner := d.getDataLogScanner(offset)

	scanner.Scan()
	dataLine := scanner.Text()
	return d.SerializeRecord(dataLine)
}

func (d *DataSegment) Write(key string, val string) int {
	byteString := []byte(CombineKeyValueForStorage(key, val))

	byteCount, err := d.Fdr.Write(byteString)
	if err != nil {
		panic(err)
	}

	return byteCount
}

func (d *DataSegment) Parse(exec func(k string, v string, byteOffset int64)) {
	scanner := d.getDataLogScanner(nil)

	var byteOffset int64 = constants.DataSegmentMetaDataByteSize

	for scanner.Scan() {
		dataLine := scanner.Text()
		record := d.SerializeRecord(dataLine)

		exec(record.Key, record.Val, byteOffset)

		byteOffset += utils.GetBlockSize(record.Key, record.Val)
	}
}

func (d *DataSegment) SerializeRecord(dataLine string) *Record {
	keyVal := strings.Split(dataLine, constants.LogKeyValDelim)
	key := keyVal[0]
	val := keyVal[1]

	return &Record{Key: key, Val: val}
}

func (d *DataSegment) GetSegmentFileCreatedAt() time.Time {
	segmentMetaDataJson := d.getMetaData()

	segmentMetaData := MetaDataDto{}
	segmentMetaData.FromByteArr(segmentMetaDataJson)

	return segmentMetaData.CreatedAt
}

func (d *DataSegment) getMetaData() []byte {
	byteArr := make([]byte, constants.DataSegmentMetaDataByteSize)

	_, err := d.Fdr.Read(byteArr)
	if err != nil {
		panic(err)
	}

	return byteArr
}

func GetDataLogScanner(f *os.File, offset *int64) *bufio.Scanner {
	var offset_ int64

	if offset == nil {
		offset_ = constants.DataSegmentMetaDataByteSize
	} else {
		offset_ = *offset
	}

	if _, err := f.Seek(offset_, 0); err != nil {
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

func ParseDataSegment(f *os.File, exec func(k string, v string, byteOffset int64)) {
	scanner := GetDataLogScanner(f, nil)

	var byteOffset int64 = constants.DataSegmentMetaDataByteSize

	for scanner.Scan() {
		dataLine := scanner.Text()

		key, val := ExtractKeyVal(dataLine)

		exec(key, val, byteOffset)

		byteOffset += utils.GetBlockSize(key, val)
	}
}

func Write(key string, val string, f *os.File) int {
	byteString := []byte(CombineKeyValueForStorage(key, val))

	byteCount, err := f.Write(byteString)
	if err != nil {
		panic(err)
	}

	return byteCount
}

func WriteMany(kvPairs [][]string, f *os.File) int {
	var sb strings.Builder

	for _, kvPair := range kvPairs {
		sb.WriteString(CombineKeyValueForStorage(kvPair[0], kvPair[1]))
	}

	byteString := []byte(sb.String())

	byteCount, err := f.Write(byteString)
	if err != nil {
		panic(err)
	}

	return byteCount
}

func CombineKeyValueForStorage(k string, v string) string {
	return k + constants.LogKeyValDelim + v + constants.LogNewLineDelim
}

func ExtractKeyVal(dataLine string) (string, string) {
	keyVal := strings.Split(dataLine, constants.LogKeyValDelim)
	key := keyVal[0]
	val := keyVal[1]
	return key, val
}

func ReadAllRecordsFromDataSegment(filePath string) []Record {
	f, deferFunc := GetLogFile(filePath, os.O_RDONLY)
	defer deferFunc(f)

	var records []Record

	ParseDataSegment(f, func(k string, v string, byteOffset int64) {
		records = append(records, Record{Key: k, Val: v})
	})

	return records
}

func GetLogFile(fileName string, flag int) (*os.File, func(file *os.File)) {
	f, err := os.OpenFile(fileName, flag, 0600)
	if err != nil {
		panic(err)
	}

	deferFunc := func(f *os.File) {
		err := f.Close()
		if err != nil && err != syscall.EBADF {
			panic(err)
		}
	}

	return f, deferFunc
}

func GetDataSegmentFileNameList(dataDirectory string) []string {
	files, err := os.ReadDir(dataDirectory)
	if err != nil {
		panic(err)
	}

	var fileNames []string

	dataFileNamePattern := "*.log"

	for _, entry := range files {
		if entry.IsDir() {
			continue
		}

		if matched, err := filepath.Match(dataFileNamePattern, entry.Name()); err != nil {
			panic(err)
		} else if matched {
			fileNames = append(fileNames, entry.Name())
		}
	}

	return fileNames
}

func ClearDataFromDirectory(directory string) error {
	dataFileNames := GetDataSegmentFileNameList(directory)

	for _, fileName := range dataFileNames {
		err := os.Remove(directory + "/" + fileName)
		if err != nil {
			return err
		}

	}

	logger.SugaredLogger.Infof("Successfully cleared all data.")
	return nil
}
