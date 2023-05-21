package test

import (
	"bitcask/commands"
	"fmt"
	"github.com/google/uuid"
	"math/rand"
)

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
