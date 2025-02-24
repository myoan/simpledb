package main

import (
	"simpledb/disk"
	"simpledb/log"
	"testing"

	mock "github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/require"
)

func TestConcurrencyMananger_SLock(t *testing.T) {
	testcases := []struct {
		name   string
		table  map[disk.Block]LockState
		expect map[disk.Block]LockState
		err    error
	}{
		{
			name:  "no one locks",
			table: map[disk.Block]LockState{},
			expect: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_SHARED,
			},
			err: nil,
		},
		{
			name: "block already shared locked",
			table: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_SHARED,
			},
			expect: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_SHARED,
			},
			err: nil,
		},
		{
			name: "block already exclusive locked",
			table: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_EXCLUSIVE,
			},
			expect: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_EXCLUSIVE,
			},
			err: ErrAlreadyLocked,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			cm := &ConcurrencyManager{lockTable: tt.table}
			blk := &disk.Block{Filename: "test.db", Num: 0}
			err := cm.SLock(blk)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expect, cm.lockTable)
		})
	}
}

func TestConcurrencyMananger_XLock(t *testing.T) {
	testcases := []struct {
		name   string
		table  map[disk.Block]LockState
		expect map[disk.Block]LockState
		err    error
	}{
		{
			name:  "no one locks",
			table: map[disk.Block]LockState{},
			expect: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_EXCLUSIVE,
			},
			err: nil,
		},
		{
			name: "block already shared locked",
			table: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_SHARED,
			},
			expect: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_SHARED,
			},
			err: ErrAlreadyLocked,
		},
		{
			name: "block already exclusive locked",
			table: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_EXCLUSIVE,
			},
			expect: map[disk.Block]LockState{
				{Filename: "test.db", Num: 0}: LockState_EXCLUSIVE,
			},
			err: ErrAlreadyLocked,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			cm := &ConcurrencyManager{lockTable: tt.table}
			blk := &disk.Block{Filename: "test.db", Num: 0}
			err := cm.XLock(blk)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expect, cm.lockTable)
		})
	}
}

type MockLogManager struct {
	mock.Mock
}

func (_m *MockLogManager) Start(txid int) error {
	ret := _m.Called(txid)
	return ret.Error(0)
}

func (_m *MockLogManager) Commit(txid int) error {
	ret := _m.Called(txid)
	return ret.Error(0)
}

func (_m *MockLogManager) Rollback(txid int) error {
	ret := _m.Called(txid)
	return ret.Error(0)
}

func (_m *MockLogManager) SetInt32(txid int, block *disk.Block, offset int, old, new int32) (int, error) {
	ret := _m.Called(txid, block, offset, old, new)
	return ret.Int(0), ret.Error(1)
}

func (_m *MockLogManager) SetString(txid int, block *disk.Block, offset int, old, new string) (int, error) {
	ret := _m.Called(txid, block, offset, old, new)
	return ret.Int(0), ret.Error(1)
}

func (_m *MockLogManager) Iterator() (*log.LogIterator, error) {
	ret := _m.Called()
	return ret.Get(0).(*log.LogIterator), ret.Error(1)
}

func TestTransaction_Start(t *testing.T) {
	fm := disk.NewNopFileManager(30, []byte{})
	lm, err := log.NewLogManager(fm, "test.db")
	mocklog := &MockLogManager{}
	mocklog.On("Start", 1).Return(nil).Once()

	require.NoError(t, err)
	cm := &ConcurrencyManager{lockTable: map[disk.Block]LockState{}}
	bm := NewBufferManager(fm, lm, 2, WithFinalizeTime(100))

	tx := NewTransaction(1, mocklog, cm, bm)
	err = tx.Start()
	require.NoError(t, err)
}

func TestTransaction_Commit(t *testing.T) {
	fm := disk.NewNopFileManager(30, []byte{})
	lm, err := log.NewLogManager(fm, "test.db")
	mocklog := &MockLogManager{}
	mocklog.On("Commit", 1).Return(nil).Once()

	require.NoError(t, err)
	cm := &ConcurrencyManager{lockTable: map[disk.Block]LockState{}}
	bm := NewBufferManager(fm, lm, 2, WithFinalizeTime(100))

	tx := NewTransaction(1, mocklog, cm, bm)
	err = tx.Commit()
	require.NoError(t, err)

	require.Equal(t, map[disk.Block]LockState{}, cm.lockTable)
	require.Empty(t, tx.locked)
}

func TestTransaction_Rollback(t *testing.T) {
	fm := disk.NewNopFileManager(30, []byte{})
	lm, err := log.NewLogManager(fm, "test.db")
	mocklog := &MockLogManager{}
	mocklog.On("Rollback", 1).Return(nil).Once()

	require.NoError(t, err)
	cm := &ConcurrencyManager{lockTable: map[disk.Block]LockState{}}
	bm := NewBufferManager(fm, lm, 2, WithFinalizeTime(100))

	tx := NewTransaction(1, mocklog, cm, bm)
	err = tx.Rollback()
	require.NoError(t, err)
	require.Empty(t, tx.locked)
}

func TestTransaction_SetInt32(t *testing.T) {
	fm := disk.NewNopFileManager(30, []byte{})
	lm, err := log.NewLogManager(fm, "test.db")
	block := disk.NewBlock("test", 0)
	mocklog := &MockLogManager{}
	mocklog.On("Start", 1).Return(nil).Once()
	mocklog.On("SetInt32", 1, block, 0, int32(0), int32(1)).Return(0, nil).Once()

	require.NoError(t, err)
	cm := &ConcurrencyManager{lockTable: map[disk.Block]LockState{}}
	bm := NewBufferManager(fm, lm, 2, WithFinalizeTime(100))

	tx := NewTransaction(1, mocklog, cm, bm)
	err = tx.Start()
	require.NoError(t, err)

	err = tx.SetInt32(block, 0, 1)
	require.NoError(t, err)

	require.Equal(t, map[disk.Block]LockState{
		{Filename: "test", Num: 0}: LockState_EXCLUSIVE,
	}, cm.lockTable)
	require.Equal(t, map[disk.Block]struct{}{
		{Filename: "test", Num: 0}: {},
	}, tx.locked)
}

func TestTransaction_SetString(t *testing.T) {
	fm := disk.NewNopFileManager(30, []byte{})
	lm, err := log.NewLogManager(fm, "test.db")
	block := disk.NewBlock("test", 0)
	mocklog := &MockLogManager{}
	mocklog.On("Start", 1).Return(nil).Once()
	mocklog.On("SetString", 1, block, 0, "", "fuga").Return(0, nil).Once()

	require.NoError(t, err)
	cm := &ConcurrencyManager{lockTable: map[disk.Block]LockState{}}
	bm := NewBufferManager(fm, lm, 2, WithFinalizeTime(100))

	tx := NewTransaction(1, mocklog, cm, bm)
	err = tx.Start()
	require.NoError(t, err)

	err = tx.SetString(block, 0, "fuga")
	require.NoError(t, err)

	require.Equal(t, map[disk.Block]LockState{
		{Filename: "test", Num: 0}: LockState_EXCLUSIVE,
	}, cm.lockTable)
	require.Equal(t, map[disk.Block]struct{}{
		{Filename: "test", Num: 0}: {},
	}, tx.locked)
}
