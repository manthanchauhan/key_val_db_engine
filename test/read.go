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
	if v != constants.ErrMsgNotFound && v != hashedV {
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
			err := r.(error)
			v = err.Error()
		}
	}()

	var err error

	if v, err = commands.GetCommandManager().ReadHandler(fmt.Sprintf("%s %s", constants.CommandRead, k)); err != nil {
		panic(err)
	}

	return v
}
