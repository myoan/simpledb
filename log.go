package main

/*
type LogManager interface {
	Append(record []byte) (int, error)
	Flush(lsn int) error
	HasNext() bool
	Next() []byte
}
*/

type LogManager struct {
	fileMng    *FileManager
	fileName   string
	page       *Page
	currentBlk *BlockID
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

	var currentblk *BlockID
	if loglen == 0 {
		currentblk, err = lm.appendNewBlock()
		if err != nil {
			return nil, err
		}
	} else {
		currentblk = NewBlockID(filename, loglen-1)
		fm.Read(currentblk, lm.page)
	}
	lm.currentBlk = currentblk
	return lm, nil
}

func (lm *LogManager) appendNewBlock() (*BlockID, error) {
	bid, err := lm.fileMng.Append(lm.fileName)
	if err != nil {
		return nil, err
	}

	err = lm.page.SetInt32(0, int32(lm.fileMng.Blocksize))
	if err != nil {
		return nil, err
	}

	err = lm.fileMng.Write(bid, lm.page)
	if err != nil {
		return nil, err
	}

	return bid, nil
}

func (lm *LogManager) Flush(lsn int) error {
	return lm.fileMng.Write(lm.currentBlk, lm.page)
}

func (lm *LogManager) Append(record []byte) (int, error) {
	lm.fileMng.Dump(lm.currentBlk)
	lm.page.SetBytes(0, record)
	return len(record) + 4, nil
}
