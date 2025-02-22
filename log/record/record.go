package record

import (
	"encoding/binary"
)

const (
	Instruction_NOP = iota
	Instruction_START
	Instruction_COMMIT
	Instruction_ROLLBACK
	Instruction_CHECKPOINT
	Instruction_NQCKPT
	Instruction_SETSTRING
	Instruction_SETINT32
)

type StartRecord struct {
	TxID int
}

func (r *StartRecord) Read(data []byte) {
	txid := binary.BigEndian.Uint32(data[4:8])
	r.TxID = int(txid)
}

type CommitRecord struct {
	TxID int
}

func (r *CommitRecord) Read(data []byte) {
	txid := binary.BigEndian.Uint32(data[4:8])
	r.TxID = int(txid)
}

type RollbackRecord struct {
	TxID int
}

func (r *RollbackRecord) Read(data []byte) {
	txid := binary.BigEndian.Uint32(data[4:8])
	r.TxID = int(txid)
}

type CheckPointRecord struct {
}

type NQCheckPointRecord struct {
	TxIDs []int
}

func (r *NQCheckPointRecord) Read(data []byte) {
	size := binary.BigEndian.Uint32(data[4:8])
	txids := make([]int, size)
	for i := 0; i < int(size); i++ {
		txid := binary.BigEndian.Uint32(data[4*i+8 : 4*i+12])
		txids[i] = int(txid)
	}
	r.TxIDs = txids
}

type SetStringRecord struct {
	TxID     int
	Filename string
	BlkNum   int
	Offset   int
	Value    string
}

func (r *SetStringRecord) Read(data []byte) {
	r.TxID = int(binary.BigEndian.Uint32(data[4:8]))
	filelen := binary.BigEndian.Uint32(data[8:12])
	r.Filename = string(data[12 : 12+filelen])
	r.BlkNum = int(binary.BigEndian.Uint32(data[12+filelen : 16+filelen]))
	r.Offset = int(binary.BigEndian.Uint32(data[16+filelen : 20+filelen]))
	valuelen := binary.BigEndian.Uint32(data[20+filelen : 24+filelen])
	r.Value = string(data[24+filelen : 24+filelen+valuelen])
}

func (r *SetStringRecord) Undo() error {
	return nil
}
