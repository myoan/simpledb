package main

import (
	"simpledb/disk"
	"testing"

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
