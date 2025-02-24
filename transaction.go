package main

import (
	"encoding/binary"
	"errors"
	"simpledb/disk"
	"simpledb/log"
	logrecord "simpledb/log/record"
	"sync"
)

type LockState int

const (
	LockState_SHARED LockState = iota
	LockState_EXCLUSIVE
)

var (
	ErrAlreadyLocked = errors.New("block already locked")
)

type ConcurrencyManager struct {
	lockTable map[disk.Block]LockState
	mu        sync.Mutex
}

func (cm *ConcurrencyManager) SLock(block *disk.Block) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	state, found := cm.lockTable[*block]
	if found {
		if state == LockState_EXCLUSIVE {
			return ErrAlreadyLocked
		} else {
			return nil
		}
	}
	cm.lockTable[*block] = LockState_SHARED
	return nil
}

func (cm *ConcurrencyManager) XLock(block *disk.Block) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, found := cm.lockTable[*block]; found {
		return ErrAlreadyLocked
	}
	cm.lockTable[*block] = LockState_EXCLUSIVE
	return nil
}

func (cm *ConcurrencyManager) Unlock(block *disk.Block) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.lockTable, *block)
}

func (cm *ConcurrencyManager) Release() {
	for block := range cm.lockTable {
		cm.Unlock(&block)
	}
}

type Transaction struct {
	id     int
	lm     log.TxLogger
	bm     *BufferManager
	cm     *ConcurrencyManager
	locked map[disk.Block]struct{}
}

func NewTransaction(id int, lm log.TxLogger, cm *ConcurrencyManager, bm *BufferManager) *Transaction {
	return &Transaction{
		id:     id,
		lm:     lm,
		cm:     cm,
		bm:     bm,
		locked: make(map[disk.Block]struct{}),
	}
}

func (tx *Transaction) Start() error {
	return tx.lm.Start(tx.id)
}

func (tx *Transaction) Commit() error {
	err := tx.lm.Commit(tx.id)
	if err != nil {
		return err
	}
	for block := range tx.locked {
		tx.cm.Unlock(&block)
	}
	return nil
}

func (tx *Transaction) Rollback() error {
	err := tx.lm.Rollback(tx.id)
	if err != nil {
		return err
	}

	itr, err := tx.lm.Iterator()
	if err != nil {
		return err
	}

	for itr.HasNext() {
		data, err := itr.Next()
		if err != nil {
			return err
		}
		inst := binary.BigEndian.Uint32(data[:4])
		switch inst {
		case logrecord.Instruction_CHECKPOINT:
			break
		case logrecord.Instruction_SETINT32:
			record := &logrecord.SetInt32Record{}
			record.Read(data)

			if record.TxID != tx.id {
				continue
			}

			buf, err := tx.bm.GetBuf(record.Block())
			if err != nil {
				return err
			}
			err = buf.Contents.SetInt32(record.Offset, record.OldValue)
			if err != nil {
				return err
			}
		case logrecord.Instruction_SETSTRING:
			record := &logrecord.SetStringRecord{}
			record.Read(data)

			if record.TxID != tx.id {
				continue
			}

			buf, err := tx.bm.GetBuf(record.Block())
			if err != nil {
				return err
			}
			err = buf.Contents.SetString(record.Offset, record.OldValue)
			if err != nil {
				return err
			}
		}
	}

	for block := range tx.locked {
		tx.cm.Unlock(&block)
	}
	return nil
}

func (tx *Transaction) GetInt32(block *disk.Block, offset int) (int32, error) {
	tx.cm.SLock(block)
	defer func() {
		tx.cm.Unlock(block)
		delete(tx.locked, *block)
	}()

	buf, err := tx.bm.GetBuf(block)
	if err != nil {
		return 0, err
	}

	return buf.Contents.GetInt32(offset)
}

func (tx *Transaction) GetString(block *disk.Block, offset int) (string, error) {
	tx.cm.SLock(block)
	defer func() {
		tx.cm.Unlock(block)
		delete(tx.locked, *block)
	}()

	buf, err := tx.bm.GetBuf(block)
	if err != nil {
		return "", err
	}
	return buf.Contents.GetString(offset)
}

func (tx *Transaction) SetInt32(block *disk.Block, offset int, n int32) error {
	tx.cm.XLock(block)

	tx.locked[*block] = struct{}{}
	buf, err := tx.bm.GetBuf(block)
	if err != nil {
		if errors.Is(err, ErrBlockNotFound) {
			buf, err = tx.bm.Pin(block)
		}
		if err != nil {
			return err
		}
	}

	content := buf.Contents
	oldval, err := content.GetInt32(offset)
	if err != nil {
		return err
	}

	lsn, err := tx.lm.SetInt32(tx.id, block, offset, oldval, n)
	if err != nil {
		return err
	}
	err = content.SetInt32(offset, n)
	if err != nil {
		return err
	}

	buf.SetModified(tx.id, lsn)
	return nil
}

func (tx *Transaction) SetString(block *disk.Block, offset int, v string) error {
	tx.cm.XLock(block)

	tx.locked[*block] = struct{}{}
	buf, err := tx.bm.GetBuf(block)
	if err != nil {
		if errors.Is(err, ErrBlockNotFound) {
			buf, err = tx.bm.Pin(block)
		}
		if err != nil {
			return err
		}
	}

	content := buf.Contents
	oldval, err := content.GetString(offset)
	if err != nil {
		return err
	}

	lsn, err := tx.lm.SetString(tx.id, block, offset, oldval, v)
	if err != nil {
		return err
	}
	err = content.SetString(offset, v)
	if err != nil {
		return err
	}

	buf.SetModified(tx.id, lsn)
	return nil
}
