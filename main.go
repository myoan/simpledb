package main

import (
	"flag"
	"fmt"
	"log/slog"
	"simpledb/log"
	"simpledb/storage"
)

const BLOCK_SIZE = 32

func main() {
	var blksize int
	flag.IntVar(&blksize, "b", BLOCK_SIZE, "block size")
	flag.Parse()

	slog.Info("simpledb started", slog.Int("blocksize", blksize))

	mng := storage.NewFileManager(blksize)
	block := storage.NewBlock("test", 0)
	page := storage.NewPage(mng.Blocksize())
	page.SetString(0, "Hello, World!")
	data, err := page.GetString(0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read from memory: %s\n", data)
	mng.Write(block, page)
	mng.Read(block, page)
	data, err = page.GetString(0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read from storage. %s\n", data)

	lm, err := log.NewLogManager(mng, "test.log")
	if err != nil {
		panic(err)
	}

	lm.Append([]byte("Hello, World!"))
	lm.Flush(lm.CurrentLSN)
}
