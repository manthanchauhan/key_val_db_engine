package test

import (
	"bitcask/commands"
	"bitcask/config/constants"
	"fmt"
	"github.com/google/uuid"
	"math/rand"
)

func write(k string, v string) {
	err := commands.GetCommandManager().WriteHandler(fmt.Sprintf("%s %s %s", constants.CommandWrite, k, v))
	if err != nil {
		panic(err)
	}
	hashMap[k] = v
}

func writeNewKey() {
	k := uuid.New().String()
	v := randStr(rand.Intn(100) + 1)
	write(k, v)
}

func writeOldKey() {
	k := pickRandomKey()
	v := randStr(rand.Intn(100) + 1)
	write(k, v)
}
