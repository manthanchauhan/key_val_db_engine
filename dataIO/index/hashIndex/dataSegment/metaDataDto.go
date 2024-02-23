package dataSegment

import (
	"bitcask/config/constants"
	"bytes"
	"encoding/json"
	"time"
)

type MetaDataDto struct {
	CreatedAt time.Time `json:"created_at"`
}

func (metaDataDto *MetaDataDto) ToByteArr() []byte {
	byteArr, err := json.Marshal(metaDataDto)

	if err != nil {
		panic(err)
	}

	paddingSize := constants.DataSegmentMetaDataByteSize - len(byteArr)
	padding := make([]byte, paddingSize)

	byteArr = append(byteArr, padding...)
	return byteArr
}

func (metaDataDto *MetaDataDto) FromByteArr(byteArr []byte) {
	byteArr = bytes.Trim(byteArr, "\x00")
	if err := json.Unmarshal(byteArr, metaDataDto); err != nil {
		panic(err)
	}
}
