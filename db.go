package main

import (
	"simpledb/disk"
	"simpledb/log"
)

type SimpleDB struct {
	BufferManager *BufferManager
	fm            disk.FileManager
	lm            *log.LogManager
}

func NewDB(filename string, blocksize, bufsize int) (*SimpleDB, error) {
	fm := disk.NewFileManager(blocksize)
	lm, err := log.NewLogManager(fm, filename+".log")
	if err != nil {
		return nil, err
	}
	bm := NewBufferManager(fm, lm, bufsize)
	return &SimpleDB{
		fm:            fm,
		lm:            lm,
		BufferManager: bm,
	}, nil
}
