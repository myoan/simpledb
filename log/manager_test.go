package log

import (
	"simpledb/disk"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireLogRecord(t *testing.T, want []byte, p *disk.Page) {
	ideal := make([]byte, len(want)+4)
	ideal[0] = 0x00
	ideal[1] = 0x00
	ideal[2] = 0x00
	ideal[3] = byte(len(want))
	copy(ideal[4:], want)
	require.Equal(t, ideal, p.Buf()[0:len(ideal)])
}

func TestLogManger_Append(t *testing.T) {
	fm := disk.NewFileManager(20)
	mng, err := NewLogManager(fm, "test.db")
	require.NoError(t, err)

	lsn, err := mng.Append([]byte("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, 4+len("Hello, World!"), lsn)
}

func TestLogManager_Start(t *testing.T) {
	fm := disk.NewFileManager(20)
	mng, err := NewLogManager(fm, "test.db")
	require.NoError(t, err)

	err = mng.Start(1)
	require.NoError(t, err)
	requireLogRecord(t, []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x01,
	}, mng.page)
}

func TestLogManager_Commit(t *testing.T) {
	fm := disk.NewFileManager(20)
	mng, err := NewLogManager(fm, "test.db")
	require.NoError(t, err)

	err = mng.Commit(1)
	require.NoError(t, err)
	requireLogRecord(t, []byte{
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x01,
	}, mng.page)
}

func TestLogManager_Rollback(t *testing.T) {
	fm := disk.NewFileManager(20)
	mng, err := NewLogManager(fm, "test.db")
	require.NoError(t, err)

	err = mng.Rollback(1)
	require.NoError(t, err)
	requireLogRecord(t, []byte{
		0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x01,
	}, mng.page)
}

func TestLogManager_SetInt32(t *testing.T) {
	fm := disk.NewFileManager(32)
	mng, err := NewLogManager(fm, "test.db")
	require.NoError(t, err)

	block := disk.NewBlock("test", 0)

	lsn, err := mng.SetInt32(1, block, 0, 10, 20)
	require.NoError(t, err)
	require.Equal(t, 28, lsn)
	requireLogRecord(t, []byte{
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x0a,
		0x00, 0x00, 0x00, 0x14,
	}, mng.page)
}

func TestLogManager_SetString(t *testing.T) {
	fm := disk.NewFileManager(40)
	mng, err := NewLogManager(fm, "test.db")
	require.NoError(t, err)

	block := disk.NewBlock("test", 0)

	lsn, err := mng.SetString(1, block, 0, "hoge", "fuga")
	require.NoError(t, err)
	require.Equal(t, 36, lsn)
	requireLogRecord(t, []byte{
		0x00, 0x00, 0x00, 0x06,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x04, 0x68, 0x6f, 0x67, 0x65,
		0x00, 0x00, 0x00, 0x04, 0x66, 0x75, 0x67, 0x61,
	}, mng.page)
}

func TestLogIterator_Next(t *testing.T) {
	testcases := []struct {
		name  string
		block *disk.Block
		fn    func() *LogManager
		want  [][]byte
	}{
		{
			name:  "start",
			block: disk.NewBlock("test", 0),
			fn: func() *LogManager {
				fm := disk.NewFileManager(40)
				mng, _ := NewLogManager(fm, "test.db")

				mng.Start(1)
				mng.Flush(1)
				return mng
			},
			want: [][]byte{
				{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01},
			},
		},
		{
			name:  "do nothing",
			block: disk.NewBlock("test", 0),
			fn: func() *LogManager {
				fm := disk.NewFileManager(40)
				mng, _ := NewLogManager(fm, "test.db")

				mng.Start(1)
				mng.Commit(1)
				mng.Flush(1)
				return mng
			},
			want: [][]byte{
				{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01}, // commit
				{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, // start
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			mng := tt.fn()
			itr, err := mng.Iterator()
			require.NoError(t, err)

			for _, expect := range tt.want {
				require.True(t, itr.HasNext())
				record, err := itr.Next()
				require.NoError(t, err)
				require.Equal(t, expect, record)
			}
			require.False(t, itr.HasNext())
		})
	}
}
