package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestLogger struct {
	result [][]byte
	itr    *LogIterator
}

func (l *TestLogger) Flush(lsn int) error {
	return nil
}

func (l *TestLogger) Append(record []byte) (int, error) {
	l.result = append(l.result, record)
	return len(record), nil
}

func (l *TestLogger) Iterator() (*LogIterator, error) {
	return l.itr, nil
}

func TestRecoveryManager_Commit(t *testing.T) {
	fm := NewFileManager(400)
	logger := &TestLogger{}

	rm, err := NewRecoveryManager(fm, logger, 1)
	require.NoError(t, err)
	err = rm.Commit()
	require.NoError(t, err)

	assert.Equal(t, 2, len(logger.result))
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, logger.result[0])
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01}, logger.result[1])
}

func TestRecoveryManager_Rollback(t *testing.T) {
	fm := NewFileManager(400)
	logger := &TestLogger{}

	rm, err := NewRecoveryManager(fm, logger, 1)
	require.NoError(t, err)
	err = rm.Rollback()
	require.NoError(t, err)

	assert.Equal(t, 2, len(logger.result))
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, logger.result[0])
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01}, logger.result[1])
}

/*
func TestRecoveryManager_Recover(t *testing.T) {
	t.Run("", func(t *testing.T) {
		rm, err := NewRecoveryManager(nil, nil, 1)
		require.NoError(t, err)
		err = rm.Recover()
		require.NoError(t, err)

	})
}
*/
