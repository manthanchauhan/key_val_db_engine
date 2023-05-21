package test

import (
	"bitcask/commands"
	"bitcask/config/constants"
	"fmt"
	"github.com/google/uuid"
)

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
