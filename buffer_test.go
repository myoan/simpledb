package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferManager_Pin(t *testing.T) {
	fm := NewFileManager(400)
	lm, err := NewLogManager(fm, "test.db")
	require.NoError(t, err)

	bm := NewBufferManager(fm, lm, 2)
	var buf *Buffer

	t.Run("success", func(t *testing.T) {
		buf, err = bm.Pin(*NewBlock("buffertest", 1))
		require.NoError(t, err)
	})

	t.Run("already pinned", func(t *testing.T) {
		_, err := bm.Pin(*NewBlock("buffertest", 1))
		require.NoError(t, err)
		_, err = bm.Pin(*NewBlock("buffertest", 1))
		require.NoError(t, err)
	})

	t.Run("unpinned", func(t *testing.T) {
		buf.Unpin()

		_, err := bm.Pin(*NewBlock("buffertest", 1))
		require.NoError(t, err)
	})

	t.Run("buffer pool is full", func(t *testing.T) {
		_, err := bm.Pin(*NewBlock("buffertest", 1))
		require.NoError(t, err)
		_, err = bm.Pin(*NewBlock("buffertest", 2))
		require.NoError(t, err)
		_, err = bm.Pin(*NewBlock("buffertest", 3))
		require.ErrorAs(t, err, &ErrBufferFull)
	})

	// TODO: if block is already in the buffer pool, return the buffer
}

func TestBuffer(t *testing.T) {
	bm := NewBufferManager(NewFileManager(400), &LogManager{}, 3, WithFinalizeTime(100))
	block0 := NewBlock("buffertest", 0)
	block1 := NewBlock("buffertest", 1)
	block2 := NewBlock("buffertest", 2)
	block3 := NewBlock("buffertest", 3)

	_, err := bm.Pin(*block0)
	require.NoError(t, err)
	buf1, err := bm.Pin(*block1)
	require.NoError(t, err)
	_, err = bm.Pin(*block2)
	require.NoError(t, err)

	bm.Unpin(buf1)

	_, err = bm.Pin(*block1)
	require.NoError(t, err)
	buf2, err := bm.Pin(*block2)
	require.NoError(t, err)

	_, err = bm.Pin(*block3)
	require.ErrorAs(t, err, &ErrBufferFull)

	bm.Unpin(buf2)

	_, err = bm.Pin(*block3)
	require.NoError(t, err)
}
