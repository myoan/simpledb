package log

import (
	"simpledb/disk"
	"simpledb/log/record"
)

/*
type LogManager interface {
	Append(record []byte) (int, error)
	Flush(lsn int) error
	HasNext() bool
	Next() []byte
}
*/

type Logger interface {
	Flush(lsn int) error
	Append(record []byte) (int, error)
	Iterator() (*LogIterator, error)
}

type LogManager struct {
	fileMng    *disk.FileManager
	fileName   string
	page       *disk.Page
	currentBlk *disk.Block
	CurrentLSN int
	savedLSN   int
}

func NewLogManager(fm *disk.FileManager, filename string) (*LogManager, error) {
	loglen, err := fm.Length(filename)
	if err != nil {
		return nil, err
	}

	lm := &LogManager{
		fileMng:  fm,
		fileName: filename,
		page:     disk.NewPage(fm.Blocksize),
	}

	var currentblk *disk.Block
	if loglen == 0 {
		currentblk, err = lm.appendNewBlock()
		if err != nil {
			return nil, err
		}
	} else {
		currentblk = disk.NewBlock(filename, loglen-1)
		fm.Read(currentblk, lm.page)
	}
	lm.currentBlk = currentblk
	return lm, nil
}

func (lm *LogManager) appendNewBlock() (*disk.Block, error) {
	block, err := lm.fileMng.Append(lm.fileName)
	if err != nil {
		return nil, err
	}

	err = lm.page.SetInt32(0, int32(lm.fileMng.Blocksize))
	if err != nil {
		return nil, err
	}

	err = lm.fileMng.Write(block, lm.page)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (lm *LogManager) Flush(lsn int) error {
	return lm.fileMng.Write(lm.currentBlk, lm.page)
}

func (lm *LogManager) Append(record []byte) (int, error) {
	err := lm.page.SetBytes(0, record)
	if err != nil {
		return 0, err
	}
	return len(record) + 4, nil
}

func (lm *LogManager) Iterator() (*LogIterator, error) {
	return NewLogIterator(lm.fileMng, lm.currentBlk)
}

func (lm *LogManager) Start(txid int) error {
	// set start log record
	// <START, txid>

	p := disk.NewPage(8)
	err := p.SetInt32(0, record.Instruction_START)
	if err != nil {
		return err
	}

	err = p.SetInt32(4, int32(txid))
	if err != nil {
		return err
	}

	_, err = lm.Append(p.Buf())
	return nil
}

func (lm *LogManager) Commit(txid int) error {
	p := disk.NewPage(8)
	err := p.SetInt32(0, record.Instruction_COMMIT)
	if err != nil {
		return err
	}

	err = p.SetInt32(4, int32(txid))
	if err != nil {
		return err
	}

	_, err = lm.Append(p.Buf())
	return nil
}

func (lm *LogManager) Rollback(txid int) error {
	p := disk.NewPage(8)
	err := p.SetInt32(0, record.Instruction_ROLLBACK)
	if err != nil {
		return err
	}

	err = p.SetInt32(4, int32(txid))
	if err != nil {
		return err
	}

	_, err = lm.Append(p.Buf())
	return nil
}

func (lm *LogManager) SetInt32(txid int, block *disk.Block, offset int, old, new int32) (int, error) {
	// <SETINT32, txid, filename, blknum, offset, oldvalue, newvalue>
	size := 24 + len(block.Filename)
	p := disk.NewPage(size)
	cur := offset
	err := p.SetInt32(cur, record.Instruction_SETINT32)
	if err != nil {
		return 0, err
	}

	cur += 4
	err = p.SetInt32(cur, int32(txid))
	if err != nil {
		return 0, err
	}

	cur += 4
	err = p.SetString(cur, block.Filename)
	if err != nil {
		return 0, err
	}

	cur += 4 + len(block.Filename)
	err = p.SetInt32(cur, int32(offset))
	if err != nil {
		return 0, err
	}

	cur += 4
	err = p.SetInt32(cur, int32(old))
	if err != nil {
		return 0, err
	}

	cur += 4
	err = p.SetInt32(cur, int32(new))
	if err != nil {
		return 0, err
	}
	_, err = lm.Append(p.Buf())
	return size, err
}

func (lm *LogManager) SetString(txid int, block *disk.Block, offset int, old, new string) (int, error) {
	// <SETSTRING, txid, filename, blknum, offset, oldvalue, newvalue>
	size := 24 + len(block.Filename) + len(old) + len(new)
	p := disk.NewPage(size)
	cur := offset
	err := p.SetInt32(cur, record.Instruction_SETSTRING)
	if err != nil {
		return 0, err
	}

	cur += 4
	err = p.SetInt32(cur, int32(txid))
	if err != nil {
		return 0, err
	}

	cur += 4
	err = p.SetString(cur, block.Filename)
	if err != nil {
		return 0, err
	}

	cur += 4 + len(block.Filename)
	err = p.SetInt32(cur, int32(offset))
	if err != nil {
		return 0, err
	}

	cur += 4
	err = p.SetString(cur, old)
	if err != nil {
		return 0, err
	}

	cur += 4 + len(old)
	err = p.SetString(cur, new)
	if err != nil {
		return 0, err
	}
	_, err = lm.Append(p.Buf())
	return size, err
}

type LogIterator struct {
	fileMng    *disk.FileManager
	block      *disk.Block
	page       *disk.Page
	currentPos int
	boundary   int
}

func NewLogIterator(fm *disk.FileManager, block *disk.Block) (*LogIterator, error) {
	page := disk.NewPage(fm.Blocksize)
	fm.Read(block, page)
	b, err := page.GetInt32(0)
	if err != nil {
		return nil, err
	}

	return &LogIterator{
		fileMng:    fm,
		block:      block,
		page:       page,
		currentPos: 4,
		boundary:   int(b),
	}, nil
}

func (i *LogIterator) HasNext() bool {
	return i.currentPos < i.fileMng.Blocksize || i.block.Num > 0
}

// Next returns the next log record order by last to first
func (i *LogIterator) Next() ([]byte, error) {
	if i.currentPos >= i.fileMng.Blocksize {
		// iterates order from the last block to the first block
		nextblock := disk.NewBlock(i.block.Filename, i.block.Num-1)
		i.fileMng.Read(nextblock, i.page)
		b, err := i.page.GetInt32(0)
		if err != nil {
			return nil, err
		}

		i.currentPos = int(b)
	}

	result, err := i.page.GetBytes(0)
	if err != nil {
		return []byte{}, err
	}

	i.currentPos += len(result)
	return result, nil
}
