package log

import (
	"simpledb/storage"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireLogRecord(t *testing.T, want []byte, p *storage.Page) {
	ideal := make([]byte, len(want)+4)
	ideal[0] = 0x00
	ideal[1] = 0x00
	ideal[2] = 0x00
	ideal[3] = byte(len(want))
	copy(ideal[4:], want)
	require.Equal(t, ideal, p.Buf[0:len(ideal)])
}

func TestLogManger_Append(t *testing.T) {
	mng, err := NewLogManager(storage.NewNopFileManager(30, []byte{}), "test.db")
	require.NoError(t, err)

	lsn, err := mng.Append([]byte("Hello"))
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f,
	}, mng.page.Buf)
	require.Equal(t, 4+len("Hello"), lsn)

	lsn, err = mng.Append([]byte("World"))
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x57, 0x6f, 0x72, 0x6c,
		0x64, 0x00, 0x00, 0x00, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f,
	}, mng.page.Buf)
	require.Equal(t, 4+len("Hello"), lsn)

	// out of bounds
	lsn, err = mng.Append([]byte("Hello, World"))
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x48, 0x65,
		0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64,
	}, mng.page.Buf)
	require.Equal(t, 4+len("Hello, World"), lsn)
}

func TestLogManager_Start(t *testing.T) {
	mng, err := NewLogManager(storage.NewNopFileManager(20, []byte{}), "test.db")
	require.NoError(t, err)

	err = mng.Start(1)
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	}, mng.page.Buf)
}

func TestLogManager_Commit(t *testing.T) {
	mng, err := NewLogManager(storage.NewNopFileManager(20, []byte{}), "test.db")
	require.NoError(t, err)

	err = mng.Commit(1)
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x08, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
	}, mng.page.Buf)
}

func TestLogManager_Rollback(t *testing.T) {
	mng, err := NewLogManager(storage.NewNopFileManager(20, []byte{}), "test.db")
	require.NoError(t, err)

	err = mng.Rollback(1)
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x08, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01,
	}, mng.page.Buf)
}

func TestLogManager_SetInt32(t *testing.T) {
	mng, err := NewLogManager(storage.NewNopFileManager(36, []byte{}), "test.db")
	require.NoError(t, err)

	block := storage.NewBlock("test", 0)

	lsn, err := mng.SetInt32(1, block, 0, 10, 20)
	require.NoError(t, err)
	require.Equal(t, 28, lsn)

	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x1c,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x0a,
		0x00, 0x00, 0x00, 0x14,
	}, mng.page.Buf)
}

func TestLogManager_SetString(t *testing.T) {
	mng, err := NewLogManager(storage.NewNopFileManager(44, []byte{}), "test.db")
	require.NoError(t, err)

	block := storage.NewBlock("test", 0)

	lsn, err := mng.SetString(1, block, 0, "hoge", "fuga")
	require.NoError(t, err)
	require.Equal(t, 36, lsn)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x04,
		0x00, 0x00, 0x00, 0x24,
		0x00, 0x00, 0x00, 0x06,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x04, 0x68, 0x6f, 0x67, 0x65,
		0x00, 0x00, 0x00, 0x04, 0x66, 0x75, 0x67, 0x61,
	}, mng.page.Buf)
}

func TestLogIterator_Next(t *testing.T) {
	testcases := []struct {
		name  string
		data  []byte
		bsize int
		block *storage.Block
		want  [][]byte
	}{
		{
			name: "start",
			data: []byte{
				0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00,
				// Start
				0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
			},
			bsize: 20,
			block: storage.NewBlock("test", 0),
			want: [][]byte{
				{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01},
			},
		},
		{
			name: "start & commit",
			data: []byte{
				0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00,
				// Commit
				0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
				// Start
				0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
			},
			bsize: 28,
			block: storage.NewBlock("test", 0),
			want: [][]byte{
				{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01}, // commit
				{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, // start
			},
		},
		{
			name: "acroos blocks",
			data: []byte{
				0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00,
				// Commit
				0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
				// Start
				0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
			},
			bsize: 28,
			block: storage.NewBlock("test", 1),
			want: [][]byte{
				{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01}, // commit
				{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, // start
				{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01}, // commit
				{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, // start
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			itr, err := NewLogIterator(storage.NewNopFileManager(tt.bsize, tt.data), tt.block)
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
