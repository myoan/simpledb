package log

import (
	"simpledb/disk"
	"testing"

	"github.com/stretchr/testify/require"
)

type nopFileManager struct {
	bsize int
}

func (d *nopFileManager) Read(blk *disk.Block, page *disk.Page) error {
	return nil
}

func (d *nopFileManager) Write(blk *disk.Block, page *disk.Page) error {
	return nil
}

func (d *nopFileManager) Append(filename string) (*disk.Block, error) {
	return nil, nil
}

func (d *nopFileManager) Length(filename string) (int, error) {
	return 0, nil
}

func (d *nopFileManager) Dump(blk *disk.Block) error {
	return nil
}

func (d *nopFileManager) Blocksize() int {
	return d.bsize
}

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
	mng, err := NewLogManager(&nopFileManager{bsize: 30}, "test.db")
	require.NoError(t, err)

	lsn, err := mng.Append([]byte("Hello"))
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f,
	}, mng.page.Buf())
	require.Equal(t, 4+len("Hello"), lsn)

	lsn, err = mng.Append([]byte("World"))
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x57, 0x6f, 0x72, 0x6c,
		0x64, 0x00, 0x00, 0x00, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f,
	}, mng.page.Buf())
	require.Equal(t, 4+len("Hello"), lsn)

	// out of bounds
	lsn, err = mng.Append([]byte("Hello, World"))
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x48, 0x65,
		0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64,
	}, mng.page.Buf())
	require.Equal(t, 4+len("Hello, World"), lsn)
}

func TestLogManager_Start(t *testing.T) {
	mng, err := NewLogManager(&nopFileManager{bsize: 20}, "test.db")
	require.NoError(t, err)

	err = mng.Start(1)
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	}, mng.page.Buf())
}

func TestLogManager_Commit(t *testing.T) {
	mng, err := NewLogManager(&nopFileManager{bsize: 20}, "test.db")
	require.NoError(t, err)

	err = mng.Commit(1)
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x08, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
	}, mng.page.Buf())
}

func TestLogManager_Rollback(t *testing.T) {
	mng, err := NewLogManager(&nopFileManager{bsize: 20}, "test.db")
	require.NoError(t, err)

	err = mng.Rollback(1)
	require.NoError(t, err)
	require.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x08, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01,
	}, mng.page.Buf())
}

func TestLogManager_SetInt32(t *testing.T) {
	mng, err := NewLogManager(&nopFileManager{bsize: 36}, "test.db")
	require.NoError(t, err)

	block := disk.NewBlock("test", 0)

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
	}, mng.page.Buf())
}

func TestLogManager_SetString(t *testing.T) {
	mng, err := NewLogManager(&nopFileManager{bsize: 44}, "test.db")
	require.NoError(t, err)

	block := disk.NewBlock("test", 0)

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
	}, mng.page.Buf())
}

func TestLogIterator_Next(t *testing.T) {
	t.Skip("先にLogRecordのindexを後ろに配置する")
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
				mng, _ := NewLogManager(&nopFileManager{bsize: 40}, "test.db")

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
				mng, _ := NewLogManager(&nopFileManager{bsize: 40}, "test.db")

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
