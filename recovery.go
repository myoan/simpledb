package main

import (
	"encoding/binary"
	"simpledb/disk"
	simpledblog "simpledb/log"
	"simpledb/log/record"
)

type RecoveryManager struct {
	fm   *disk.FileManager
	lm   simpledblog.Logger
	txid int
}

func NewRecoveryManager(fm *disk.FileManager, lm simpledblog.Logger, txid int) (*RecoveryManager, error) {
	// start record
	// <START, txid>

	p := disk.NewPage(8)
	err := p.SetInt32(0, record.Instruction_START)
	if err != nil {
		return nil, err
	}

	err = p.SetInt32(0, int32(txid))
	if err != nil {
		return nil, err
	}

	_, err = lm.Append(p.Buf())
	return &RecoveryManager{
		fm:   fm,
		lm:   lm,
		txid: txid,
	}, nil
}

func (rm *RecoveryManager) Start() {}

func (rm *RecoveryManager) Commit() error {
	// <COMMIT, txid>
	p := disk.NewPage(8)
	err := p.SetInt32(0, record.Instruction_COMMIT)
	if err != nil {
		return err
	}

	err = p.SetInt32(0, int32(rm.txid))
	if err != nil {
		return err
	}

	_, err = rm.lm.Append(p.Buf())
	return err
}

func (rm *RecoveryManager) Rollback() error {
	// <ROLLBACK, txid>
	p := disk.NewPage(8)
	err := p.SetInt32(0, record.Instruction_ROLLBACK)
	if err != nil {
		return err
	}

	err = p.SetInt32(0, int32(rm.txid))
	if err != nil {
		return err
	}

	_, err = rm.lm.Append(p.Buf())
	return err
}

func (rm *RecoveryManager) Recover() error {
	// list uncommitted txid
	itr, err := rm.lm.Iterator()
	if err != nil {
		return err
	}

	for itr.HasNext() {
		data, err := itr.Next()
		if err != nil {
			return err
		}
		inst := binary.BigEndian.Uint32(data[0:4])
		switch inst {
		case record.Instruction_START:
			rec := record.StartRecord{}
			rec.Read(data)
		case record.Instruction_COMMIT:
			rec := record.CommitRecord{}
			rec.Read(data)
			// delete txid
		case record.Instruction_ROLLBACK:
			rec := record.RollbackRecord{}
			rec.Read(data)
			// delete txid
		case record.Instruction_CHECKPOINT:
			break
		case record.Instruction_SETSTRING:
			rec := record.SetStringRecord{}
			rec.Read(data)
		}
	}

	// _, err = rm.lm.Append(p.buf)
	return nil
}

func (rm *RecoveryManager) SetInt32(block *disk.Block, offset int, oldv, newv int32) (int, error) {
	// <SETINT32, txid, filename, blknum, offset, oldvalue, newvalue>

	return 0, nil
}

func (rm *RecoveryManager) SetString(buf *Buffer, offset int, v string) error {
	// <SETSTRING, txid, filename, blknum, offset, value>
	return nil
}
