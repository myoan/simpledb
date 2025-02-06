package main

import (
	"flag"
	"fmt"
	"log/slog"
)

const BLOCK_SIZE = 32

func main() {
	var blksize int
	flag.IntVar(&blksize, "b", BLOCK_SIZE, "block size")
	flag.Parse()

	slog.Info("simpledb started", slog.Int("blocksize", blksize))

	mng := NewFileManager(blksize)
	bid := NewBlockID("test", 0)
	page := NewPage(mng.Blocksize)
	page.SetString(0, "Hello, World!")
	data, err := page.GetString(0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read from memory: %s\n", data)
	mng.Write(bid, page)
	mng.Read(bid, page)
	data, err = page.GetString(0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read from disk: %s\n", data)

	lm, err := NewLogManager(mng, "test.log")
	if err != nil {
		panic(err)
	}

	lm.Append([]byte("Hello, World!"))
	lm.Flush(lm.currentLSN)
}
