package main

import (
	"errors"
	"time"
)

const (
	FinalizeTimeMs = 10000 // 10 seconds
)

var ErrBufferFull = errors.New("no available buffer")

type BufferManager struct {
	Available int
	pool      []*Buffer
	final     int
	fm        *FileManager
	lm        *LogManager
	count     int
}

type BufferManagerOptions func(bm *BufferManager)

func WithFinalizeTime(ms int) BufferManagerOptions {
	return func(bm *BufferManager) {
		bm.final = ms
	}
}

func NewBufferManager(fm *FileManager, lm *LogManager, bufCnt int, opts ...BufferManagerOptions) *BufferManager {
	pool := make([]*Buffer, bufCnt)
	for i := 0; i < bufCnt; i++ {
		pool[i] = NewBuffer(fm, lm)
	}

	mng := &BufferManager{
		Available: bufCnt,
		pool:      pool,
		final:     FinalizeTimeMs,
		fm:        fm,
		lm:        lm,
		count:     bufCnt,
	}

	for _, opt := range opts {
		opt(mng)
	}

	return mng
}

func (bm *BufferManager) GetBuf(bid *Block) (*Buffer, error) {
	for _, buf := range bm.pool {
		if buf.block != nil && buf.block.Equals(bid) {
			return buf, nil
		}
	}
	return nil, errors.New("block not found")
}

func (bm *BufferManager) FlushAll(txnum int) {
	for _, buf := range bm.pool {
		if buf.ModifyingTx() == txnum {
			buf.Flush()
		}
	}
}

// Pin 指定したblockをbufferに読み込む
func (bm *BufferManager) Pin(bid Block) (*Buffer, error) {
	// check if the block is already in the buffer pool
	for _, bp := range bm.pool {
		if bp.block != nil && bp.block.Equals(&bid) {
			if bp.IsPinned() {
				// allocate another buffer
				return bp, nil
			} else {
				// repin and return the buffer
				bp.Pin()
				return bp, nil
			}
		}
	}

	// check if the block is not in the buffer pool
	remain := bm.final
	for {
		if remain < 0 {
			// buffer pool is full, return error
			return nil, ErrBufferFull
		}

		// check if there is an unpinned buffer
		// this is naive implementation
		for _, buf := range bm.pool {
			if !buf.IsPinned() {
				buf.Flush()
				buf.block = &bid
				buf.Pin()
				return buf, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
		remain -= 10
	}
}

func (bm *BufferManager) Unpin(buf *Buffer) {
	buf.Unpin()
	if !buf.IsPinned() {
		bm.Available++
	}
}

type Buffer struct {
	Contents *Page
	fm       *FileManager
	lm       *LogManager
	block    *Block
	pincnt   int
	txnum    int
	lsn      int
}

func NewBuffer(fm *FileManager, lm *LogManager) *Buffer {
	c := NewPage(int(fm.Blocksize))
	return &Buffer{
		fm:       fm,
		lm:       lm,
		Contents: c,
		pincnt:   0,
		txnum:    -1,
		lsn:      -1,
	}
}

func (b *Buffer) Block() *Block {
	return b.block
}

func (b *Buffer) IsPinned() bool {
	return b.pincnt > 0
}

func (b *Buffer) SetModified(txnum, lsn int) {
	b.txnum = txnum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) ModifyingTx() int {
	return b.txnum
}

func (b *Buffer) Flush() {
	if b.txnum >= 0 {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.block, b.Contents)
		b.txnum = -1
	}
}

func (b *Buffer) Pin() {
	b.pincnt++
}
func (b *Buffer) Unpin() {
	b.pincnt--
}
