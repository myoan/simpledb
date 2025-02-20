package main

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
	fileMng    *FileManager
	fileName   string
	page       *Page
	currentBlk *Block
	currentLSN int
	savedLSN   int
}

func NewLogManager(fm *FileManager, filename string) (*LogManager, error) {
	loglen, err := fm.Length(filename)
	if err != nil {
		return nil, err
	}

	lm := &LogManager{
		fileMng:  fm,
		fileName: filename,
		page:     NewPage(fm.Blocksize),
	}

	var currentblk *Block
	if loglen == 0 {
		currentblk, err = lm.appendNewBlock()
		if err != nil {
			return nil, err
		}
	} else {
		currentblk = NewBlock(filename, loglen-1)
		fm.Read(currentblk, lm.page)
	}
	lm.currentBlk = currentblk
	return lm, nil
}

func (lm *LogManager) appendNewBlock() (*Block, error) {
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
	lm.fileMng.Dump(lm.currentBlk)
	lm.page.SetBytes(0, record)
	return len(record) + 4, nil
}

func (lm *LogManager) Iterator() (*LogIterator, error) {
	return NewLogIterator(lm.fileMng, lm.currentBlk)
}

type LogIterator struct {
	fileMng    *FileManager
	block      *Block
	page       *Page
	currentPos int
	boundary   int
}

func NewLogIterator(fm *FileManager, block *Block) (*LogIterator, error) {
	page := NewPage(fm.Blocksize)
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
		nextblock := NewBlock(i.block.Filename, i.block.Num-1)
		i.fileMng.Read(nextblock, i.page)
		b, err := i.page.GetInt32(0)
		if err != nil {
			return nil, err
		}

		i.currentPos = int(b)
		i.boundary = int(b)
	}

	result, err := i.page.GetBytes(i.currentPos)
	if err != nil {
		return []byte{}, err
	}

	i.currentPos += len(result) + 4
	return result, nil
}
