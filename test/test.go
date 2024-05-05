package test

import (
	"bitcask/config/constants"
	"bitcask/utils"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

var hashMap = map[string]string{}
var testHashMap = map[int]int{}

func RunTests() {
	defer func() {
		if r := recover(); r != nil {
			panic(r)
		}
	}()

	runTests()
}

func ClearDataLogs() {
	err := utils.ClearDataFromDirectory(utils.GetDataDirectory())
	if err != nil {
		panic(err)
	}

	err = utils.ClearDataFromDirectory(utils.GetDataDirectoryForIndex(constants.IndexTypeLSMIndex) + "/WALs")
	if err != nil {
		panic(err)
	}
}

func runTests() {
	println("Running tests")

	startTime := time.Now()

	for iterations := 0; iterations < 10000; iterations++ {
		randInt := rand.Int()

		divisor := 4

		if len(hashMap) == 0 {
			divisor = 2
		}

		switch randInt % divisor {
		case 0:
			testHashMap[0]++
			writeNewKey()
		case 1:
			testHashMap[1]++
			readNewKey()
		case 2:
			testHashMap[2]++
			writeOldKey()
		case 3:
			testHashMap[3]++
			readOldKey()
		}
	}

	endTime := time.Now()

	fmt.Printf("Tests took %s\n", endTime.Sub(startTime))

	println()
	for k, v := range testHashMap {
		testName := ""

		switch k {
		case 0:
			testName = "WRITE NEW"
		case 1:
			testName = "READ NEW"
		case 2:
			testName = "WRITE OLD"
		case 3:
			testName = "READ OLD"
		}
		fmt.Printf("%s - %s\n", testName, strconv.Itoa(v))
	}
	println()

	println("tests completed successfully")
}

func pickRandomKey() string {
	var keys []string

	for k := range hashMap {
		keys = append(keys, k)
	}

	return keys[rand.Intn(len(keys))]
}

func randStr(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
