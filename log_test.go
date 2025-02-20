package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogManger_Append(t *testing.T) {
	fm := NewFileManager(400)
	mng, err := NewLogManager(fm, "test.db")
	require.NoError(t, err)

	lsn, err := mng.Append([]byte("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, 4+len("Hello, World!"), lsn)
}
