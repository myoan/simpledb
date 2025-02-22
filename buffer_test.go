package main

import (
	"simpledb/disk"
	"simpledb/log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferManager_Pin(t *testing.T) {
	fm := disk.NewFileManager(400)
	lm, err := log.NewLogManager(fm, "test.db")
	require.NoError(t, err)

	bm := NewBufferManager(fm, lm, 2, WithFinalizeTime(100))
	var buf *Buffer

	t.Run("success", func(t *testing.T) {
		buf, err = bm.Pin(disk.NewBlock("buffertest", 1))
		require.NoError(t, err)
	})

	t.Run("already pinned", func(t *testing.T) {
		_, err := bm.Pin(disk.NewBlock("buffertest", 1))
		require.NoError(t, err)
		_, err = bm.Pin(disk.NewBlock("buffertest", 1))
		require.NoError(t, err)
	})

	t.Run("unpinned", func(t *testing.T) {
		buf.Unpin()

		_, err := bm.Pin(disk.NewBlock("buffertest", 1))
		require.NoError(t, err)
	})

	t.Run("buffer pool is full", func(t *testing.T) {
		_, err := bm.Pin(disk.NewBlock("buffertest", 1))
		require.NoError(t, err)
		_, err = bm.Pin(disk.NewBlock("buffertest", 2))
		require.NoError(t, err)
		_, err = bm.Pin(disk.NewBlock("buffertest", 3))
		require.ErrorAs(t, err, &ErrBufferFull)
	})

	// TODO: if block is already in the buffer pool, return the buffer
}

func TestBuffer(t *testing.T) {
	bm := NewBufferManager(disk.NewFileManager(400), &log.LogManager{}, 3, WithFinalizeTime(100))
	blk0 := disk.NewBlock("buffertest", 0)
	blk1 := disk.NewBlock("buffertest", 1)
	blk2 := disk.NewBlock("buffertest", 2)
	blk3 := disk.NewBlock("buffertest", 3)

	_, err := bm.Pin(blk0)
	require.NoError(t, err)
	buf1, err := bm.Pin(blk1)
	require.NoError(t, err)
	_, err = bm.Pin(blk2)
	require.NoError(t, err)

	bm.Unpin(buf1)

	_, err = bm.Pin(blk1)
	require.NoError(t, err)
	buf2, err := bm.Pin(blk2)
	require.NoError(t, err)

	_, err = bm.Pin(blk3)
	require.ErrorAs(t, err, &ErrBufferFull)

	bm.Unpin(buf2)

	_, err = bm.Pin(blk3)
	require.NoError(t, err)
}
