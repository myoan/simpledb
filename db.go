package main

import (
	"simpledb/log"
	"simpledb/storage"
)

type SimpleDB struct {
	BufferManager *BufferManager
	fm            storage.FileManager
	lm            *log.LogManager
}

func NewDB(filename string, blocksize, bufsize int) (*SimpleDB, error) {
	fm := storage.NewFileManager(blocksize)
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
