package test

import (
	"bitcask/commands"
	"bitcask/config/constants"
	"bitcask/utils"
	"fmt"
	"math/rand"
	"os"
)
import "github.com/google/uuid"

var hashMap = map[string]string{}
var testHashMap = map[int]int{}

func RunTests() {
	defer func() {
		if r := recover(); r != nil {
			err := os.RemoveAll(utils.GetDataDirectory())
			if err != nil {
				panic(err)
			}

			err = os.Mkdir(utils.GetDataDirectory(), 0777)
			if err != nil {
				panic(err)
			}

			panic(r)
		}
	}()

	runTests()

	err := os.RemoveAll(utils.GetDataDirectory())
	if err != nil {
		panic(err)
	}

	err = os.Mkdir(utils.GetDataDirectory(), 0777)
	if err != nil {
		panic(err)
	}
}

func runTests() {
	println("Running tests")

	for iterations := 0; iterations < 100000; iterations++ {
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

	print(testHashMap)

	println("tests completed successfully")
}

func write(k string, v string) {
	commands.WriteCommand(fmt.Sprintf("WRITE %s %s", k, v))
	hashMap[k] = v
}

func writeNewKey() {
	k := uuid.New().String()
	v := randStr(rand.Intn(100))
	write(k, v)
}

func writeOldKey() {
	k := pickRandomKey()
	v := randStr(rand.Intn(100))
	write(k, v)
}

func readNewKey() {
	k := uuid.New().String()
	v := read(k)

	hashedV := hashMap[k]
	if v != constants.NotFoundMsg && v != hashedV {
		panic("Err")
	}
}

func readOldKey() {
	k := pickRandomKey()
	v := read(k)

	hashedV := hashMap[k]

	if v != hashedV {
		panic("Err")
	}
}

func read(k string) (v string) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool

			if v, ok = r.(string); !ok {
				panic(r)
			}
		}
	}()

	v = commands.ReadCommand(fmt.Sprintf("READ %s", k))

	return v
}

func pickRandomKey() string {
	var keys []string

	for k, _ := range hashMap {
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
