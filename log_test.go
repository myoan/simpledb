package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogManger_Append(t *testing.T) {
	mng := &LogManager{}
	lsn, err := mng.Append([]byte("Hello, World!"))
	require.NoError(t, err)
	require.Equal(t, len("Hello, World!"), lsn)
}
