package compressAndMerge

import (
	"bitcask/dataIO"
	"errors"
	"io/fs"
	"syscall"
	"time"
)

func CompressionAndMergingGoRoutine() {
	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-ticker.C:
			compressAndMerge()
		}
	}
}

func compressAndMerge() {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(*fs.PathError); ok && errors.Is(err.Err, syscall.ENOENT) {
				return
			} else {
				panic(r)
			}
		}
	}()

	dataIOManager := dataIO.GetDataIOManager()
	dataIOManager.CompressAndMerge()
}
