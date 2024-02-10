package utils

import (
	"bitcask/config/constants"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func GetDataLocationFromByteOffset(segmentFileName string, byteOffset int64) string {
	return fmt.Sprintf("%s:%s", segmentFileName, strconv.FormatInt(byteOffset, 10))
}

func GetBlockSize(key string, val string) int64 {
	return int64(len(key)) + int64(len(val)) + int64(len(constants.LogKeyValDelim)) + int64(len(constants.LogNewLineDelim))
}

func GetDataDirectory() string {
	if IsExecutionModeProduction() {
		return "/Users/manthan/GolandProjects/bitcask/dataLogs/"
	} else {
		return "/Users/manthan/GolandProjects/bitcask/dataLogsTest/"
	}
}

func IsExecutionModeProduction() bool {
	return os.Getenv(constants.ModeEnvVar) == "prod"
}

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func GetClientType() string {
	return os.Getenv(constants.ClientType)
}

func EqualsIgnoreCase(s1 string, s2 string) bool {
	s1 = strings.ToUpper(strings.TrimSpace(s1))
	s2 = strings.ToUpper(strings.TrimSpace(s2))

	return s1 == s2
}
