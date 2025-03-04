package log

import (
	"errors"
	"simpledb/log/record"
	"simpledb/storage"
)

type Logger interface {
	Flush(lsn int) error
	Append(record []byte) (int, error)
	Iterator() (*LogIterator, error)
}

type TxLogger interface {
	Iterator() (*LogIterator, error)
	Start(txid int) error
	Commit(txid int) error
	Rollback(txid int) error
	SetInt32(txid int, block *storage.Block, offset int, old, new int32) (int, error)
	SetString(txid int, block *storage.Block, offset int, old, new string) (int, error)
}

type LogManager struct {
	fileMng    storage.FileManager
	fileName   string
	page       *storage.Page
	currentBlk *storage.Block
	CurrentLSN int
	savedLSN   int
}

func NewLogManager(fm storage.FileManager, filename string) (*LogManager, error) {
	loglen, err := fm.Length(filename)
	if err != nil {
		return nil, err
	}

	lm := &LogManager{
		fileMng:  fm,
		fileName: filename,
		page:     storage.NewPage(fm.Blocksize()),
	}

	var currentblk *storage.Block
	if loglen == 0 {
		currentblk, err = lm.appendNewBlock()
		if err != nil {
			return nil, err
		}
	} else {
		currentblk = storage.NewBlock(filename, loglen-1)
		fm.Read(currentblk, lm.page)
	}
	lm.currentBlk = currentblk
	return lm, nil
}

func (lm *LogManager) appendNewBlock() (*storage.Block, error) {
	block, err := lm.fileMng.Append(lm.fileName)
	if err != nil {
		return nil, err
	}

	err = lm.page.SetInt32(0, int32(lm.fileMng.Blocksize()))
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
	err := lm.fileMng.Write(lm.currentBlk, lm.page)
	if err != nil {
		return err
	}
	if lsn > lm.CurrentLSN {
		lm.savedLSN = lm.CurrentLSN
	} else {
		lm.savedLSN = lsn
	}
	return nil
}

// Append appends a log record to the end of the log file
func (lm *LogManager) Append(record []byte) (int, error) {
	var boundary int32
	var index int
	var err error
	boundary, err = lm.page.GetInt32(0)
	if err != nil {
		return 0, err
	}
	index = int(boundary) - len(record) - 4
	if index == 0 {
		// TODO: blocksizeとrecordのサイズが同じ場合の処理
		return 0, errors.New("blocksize and record size are same")
	} else if index < 0 {
		err = lm.Flush(lm.CurrentLSN)
		if err != nil {
			return 0, err
		}
		lm.appendNewBlock()
		err = lm.page.SetInt32(0, int32(lm.fileMng.Blocksize()))
		if err != nil {
			return 0, err
		}
		boundary = int32(lm.fileMng.Blocksize())
		index = int(boundary) - len(record) - 4
	}
	err = lm.page.SetBytes(index, record)
	if err != nil {
		return 0, err
	}
	err = lm.page.SetInt32(0, int32(index))
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

	p := storage.NewPage(8)
	err := p.SetInt32(0, record.Instruction_START)
	if err != nil {
		return err
	}

	err = p.SetInt32(4, int32(txid))
	if err != nil {
		return err
	}

	_, err = lm.Append(p.Buf)
	return nil
}

func (lm *LogManager) Commit(txid int) error {
	p := storage.NewPage(8)
	err := p.SetInt32(0, record.Instruction_COMMIT)
	if err != nil {
		return err
	}

	err = p.SetInt32(4, int32(txid))
	if err != nil {
		return err
	}

	_, err = lm.Append(p.Buf)
	return nil
}

func (lm *LogManager) Rollback(txid int) error {
	p := storage.NewPage(8)
	err := p.SetInt32(0, record.Instruction_ROLLBACK)
	if err != nil {
		return err
	}

	err = p.SetInt32(4, int32(txid))
	if err != nil {
		return err
	}

	_, err = lm.Append(p.Buf)
	return nil
}

func (lm *LogManager) SetInt32(txid int, block *storage.Block, offset int, old, new int32) (int, error) {
	// <SETINT32, txid, filename, blknum, offset, oldvalue, newvalue>
	size := 24 + len(block.Filename)
	p := storage.NewPage(size)
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
	_, err = lm.Append(p.Buf)
	return size, err
}

func (lm *LogManager) SetString(txid int, block *storage.Block, offset int, old, new string) (int, error) {
	// <SETSTRING, txid, filename, blknum, offset, oldvalue, newvalue>
	size := 24 + len(block.Filename) + len(old) + len(new)
	p := storage.NewPage(size)
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
	_, err = lm.Append(p.Buf)
	return size, err
}

type LogIterator struct {
	fileMng    storage.FileManager
	block      *storage.Block
	page       *storage.Page
	currentPos int
}

func NewLogIterator(fm storage.FileManager, block *storage.Block) (*LogIterator, error) {
	page := storage.NewPage(fm.Blocksize())
	fm.Read(block, page)
	b, err := page.GetInt32(0)
	if err != nil {
		return nil, err
	}

	return &LogIterator{
		fileMng:    fm,
		block:      block,
		page:       page,
		currentPos: int(b),
	}, nil
}

func (i *LogIterator) HasNext() bool {
	return i.currentPos < i.fileMng.Blocksize() || i.block.Num > 0
}

// Next returns the next log record order by last to first
func (i *LogIterator) Next() ([]byte, error) {
	if i.currentPos >= i.fileMng.Blocksize() {
		// iterates order from the last block to the first block
		nextblock := storage.NewBlock(i.block.Filename, i.block.Num-1)
		i.fileMng.Read(nextblock, i.page)
		b, err := i.page.GetInt32(0)
		if err != nil {
			return nil, err
		}

		i.currentPos = int(b)
		i.block = nextblock
	}

	result, err := i.page.GetBytes(i.currentPos)
	if err != nil {
		return []byte{}, err
	}

	i.currentPos += len(result) + 4
	return result, nil
}
