package utils

import (
	"bitcask/config/constants"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

func GetDataLocationFromByteOffset(segmentFileName string, byteOffset int64) string {
	return fmt.Sprintf("%s:%s", segmentFileName, strconv.FormatInt(byteOffset, 10))
}

func GetBlockSize(key string, val string) int64 {
	return int64(len(key)) + int64(len(val)) + int64(len(constants.LogKeyValDelim)) + int64(len(constants.LogNewLineDelim))
}

func GetDataDirectory() string {
	if IsExecutionModeProduction() {
		switch GetIndexType() {
		case constants.IndexTypeLSMIndex:
			return constants.DataDirectoryLSMIndex
		default:
			return constants.DataDirectoryHashIndex
		}
	} else {
		switch GetIndexType() {
		case constants.IndexTypeLSMIndex:
			return constants.DataDirectoryLSMIndexTest
		default:
			return constants.DataDirectoryHashIndexTest
		}
	}
}

func GetDataDirectoryForIndex(indexType string) string {
	if IsExecutionModeProduction() {
		switch indexType {
		case constants.IndexTypeLSMIndex:
			return constants.DataDirectoryLSMIndex
		default:
			return constants.DataDirectoryHashIndex
		}
	} else {
		switch indexType {
		case constants.IndexTypeLSMIndex:
			return constants.DataDirectoryLSMIndexTest
		default:
			return constants.DataDirectoryHashIndexTest
		}
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
	var envClientType = os.Getenv(constants.ClientType)

	if envClientType == "" {
		return constants.ClientTypeShell
	}

	return envClientType
}

func GetIndexType() string {
	var envIndexType = os.Getenv(constants.IndexType)

	if envIndexType == "" {
		return constants.IndexTypeLSMIndex
	}
	return envIndexType
}

func LockThenDefer(mutex *sync.RWMutex) func() {
	mutex.Lock()

	return func() {
		mutex.Unlock()
	}
}

func EqualsIgnoreCase(s1 string, s2 string) bool {
	s1 = strings.ToUpper(strings.TrimSpace(s1))
	s2 = strings.ToUpper(strings.TrimSpace(s2))

	return s1 == s2
}

func ValidateNotProtectedKeyword(val string) error {
	val = strings.TrimSpace(val)

	for _, keyword := range constants.Keywords {
		if strings.ToUpper(keyword) == strings.ToUpper(val) {
			return errors.New(fmt.Sprintf(constants.ErrMsgProtectedKeyword, val))
		}
	}

	return nil
}

func GetExecutionMode() string {
	return os.Getenv(constants.ModeEnvVar)
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
