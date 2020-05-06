package app

import (
	"bytes"
	"encoding/gob"
	"hash/crc32"
)

func HashUInt64(key uint64) uint64 {
	buff := new(bytes.Buffer)
	encoder := gob.NewEncoder(buff)
	if err := encoder.Encode(key); err != nil {
		panic(err)
	}
	return uint64(crc32.ChecksumIEEE(buff.Bytes()))
}
