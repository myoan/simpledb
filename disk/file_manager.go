package disk

import (
	"errors"
	"log/slog"
	"os"
)

type FileManager interface {
	Read(block *Block, page *Page) error
	Write(block *Block, page *Page) error
	Append(filename string) (*Block, error)
	Length(filename string) (int, error)
	Dump(block *Block) error
	Blocksize() int
}

type fileManager struct {
	blocksize int
}

func NewFileManager(blocksize int) FileManager {
	return &fileManager{
		blocksize: blocksize,
	}
}

func (fm *fileManager) Read(block *Block, page *Page) error {
	f, err := os.Open(block.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.ReadAt(page.Buf, int64(block.Num)*int64(fm.blocksize))
	return err
}

func (fm *fileManager) Write(block *Block, page *Page) error {
	slog.Info("FileManager.Write", slog.String("block", block.Filename), slog.Int("num", block.Num))
	f, err := os.OpenFile(block.Filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteAt(page.Buf, int64(block.Num)*int64(fm.blocksize))
	return err
}

// Append appends a empty page to the end of the file
func (fm *fileManager) Append(filename string) (*Block, error) {
	blklen, err := fm.Length(filename)
	if err != nil {
		return nil, err
	}

	blk := NewBlock(filename, blklen)

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data := make([]byte, fm.blocksize)
	f.Seek(int64(blklen*fm.blocksize), 0)
	f.Write(data)

	return blk, nil
}

// Length returns the number of blocks in the file
func (fm *fileManager) Length(filename string) (int, error) {
	info, err := os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	return int(info.Size()) / fm.blocksize, err
}

func (fm *fileManager) Dump(block *Block) error {
	f, err := os.Open(block.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, fm.blocksize)
	_, err = f.ReadAt(buf, int64(block.Num)*int64(fm.Blocksize()))
	if err != nil {
		return err
	}

	slog.Info("FileManager.Dump", slog.String("filename", block.Filename), slog.Int("num", block.Num), slog.String("data", string(buf)))
	return nil
}

func (fm *fileManager) Blocksize() int {
	return fm.blocksize
}

type NopFileManager struct {
	data  []byte
	bsize int
}

func NewNopFileManager(bsize int, data []byte) *NopFileManager {
	return &NopFileManager{
		bsize: bsize,
		data:  data,
	}
}

func (d *NopFileManager) Read(blk *Block, page *Page) error {
	page.Buf = d.data
	return nil
}

func (d *NopFileManager) Write(blk *Block, page *Page) error {
	return nil
}

func (d *NopFileManager) Append(filename string) (*Block, error) {
	return nil, nil
}

func (d *NopFileManager) Length(filename string) (int, error) {
	return 0, nil
}

func (d *NopFileManager) Dump(blk *Block) error {
	return nil
}

func (d *NopFileManager) Blocksize() int {
	return d.bsize
}
