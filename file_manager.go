package main

import (
	"errors"
	"log/slog"
	"os"
)

type FileManager struct {
	Blocksize int
}

func NewFileManager(blocksize int) *FileManager {
	return &FileManager{
		Blocksize: blocksize,
	}
}

func (fm *FileManager) Read(bid *BlockID, page *Page) error {
	f, err := os.Open(bid.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.ReadAt(page.buf, int64(bid.Num)*int64(fm.Blocksize))
	return err
}

func (fm *FileManager) Write(bid *BlockID, page *Page) error {
	slog.Info("FileManager.Write", slog.String("bid", bid.Filename), slog.Int("num", bid.Num))
	f, err := os.OpenFile(bid.Filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteAt(page.buf, int64(bid.Num)*int64(fm.Blocksize))
	return err
}

// Append appends a empty page to the end of the file
func (fm *FileManager) Append(filename string) (*BlockID, error) {
	blklen, err := fm.Length(filename)
	if err != nil {
		return nil, err
	}

	blk := NewBlockID(filename, blklen)

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data := make([]byte, fm.Blocksize)
	f.Seek(int64(blklen*fm.Blocksize), 0)
	f.Write(data)

	return blk, nil
}

// Length returns the number of blocks in the file
func (fm *FileManager) Length(filename string) (int, error) {
	info, err := os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return int(info.Size()) / fm.Blocksize, err
}

func (fm *FileManager) Dump(bid *BlockID) error {
	f, err := os.Open(bid.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, fm.Blocksize)
	_, err = f.ReadAt(buf, int64(bid.Num)*int64(fm.Blocksize))
	if err != nil {
		return err
	}

	slog.Info("FileManager.Dump", slog.String("filename", bid.Filename), slog.Int("num", bid.Num), slog.String("data", string(buf)))
	return nil
}
