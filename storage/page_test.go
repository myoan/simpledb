package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetInt32(t *testing.T) {
	tests := []struct {
		name      string
		pageData  []byte
		offset    int
		want      int32
		cursor    int
		expectErr error
	}{
		{
			name:      "valid offset",
			pageData:  []byte{0x00, 0x00, 0x00, 0x01},
			offset:    0,
			want:      1,
			cursor:    4,
			expectErr: nil,
		},
		{
			name:      "out of bounds offset",
			pageData:  []byte{0x00, 0x00, 0x00, 0x01},
			offset:    2,
			want:      0,
			cursor:    0,
			expectErr: ErrOutOfBounds,
		},
		{
			name:      "negative offset",
			pageData:  []byte{0x00, 0x00, 0x00, 0x01},
			offset:    -1,
			want:      0,
			cursor:    0,
			expectErr: ErrOutOfBounds,
		},
		{
			name:      "negative number",
			pageData:  []byte{0xff, 0xff, 0xff, 0xff},
			offset:    0,
			want:      -1,
			cursor:    4,
			expectErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Page{Buf: tt.pageData}
			got, err := p.GetInt32(tt.offset)
			assert.Equal(t, tt.expectErr, err)
			assert.Equal(t, tt.want, got)
			// assert.Equal(t, tt.cursor, p.cursor)
		})
	}
}

func TestGetInt32_continuous(t *testing.T) {
	pagedata := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03,
	}
	p := &Page{Buf: pagedata}

	for i := 0; i < 3; i++ {
		got, err := p.GetInt32(i * 4)
		assert.NoError(t, err)
		assert.Equal(t, int32(i+1), got)
	}
}
func TestSetInt32(t *testing.T) {
	tests := []struct {
		name      string
		pagesize  int
		offset    int
		value     int32
		cursor    int
		want      []byte
		expectErr error
	}{
		{
			name:      "valid set",
			pagesize:  4,
			offset:    0,
			value:     1,
			cursor:    4,
			want:      []byte{0x00, 0x00, 0x00, 0x01},
			expectErr: nil,
		},
		{
			name:      "enougth buffer",
			pagesize:  8,
			offset:    0,
			value:     1,
			cursor:    4,
			want:      []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			expectErr: nil,
		},
		{
			name:      "enougth buffer with offset",
			pagesize:  8,
			offset:    4,
			value:     1,
			cursor:    8,
			want:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
			expectErr: nil,
		},
		{
			name:      "buffer too small",
			pagesize:  2,
			offset:    0,
			value:     1,
			cursor:    0,
			want:      []byte{0x00, 0x00},
			expectErr: ErrOutOfBounds,
		},
		{
			name:      "negative number",
			pagesize:  4,
			offset:    0,
			value:     -1,
			cursor:    4,
			want:      []byte{0xff, 0xff, 0xff, 0xff},
			expectErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPage(tt.pagesize)
			err := p.SetInt32(tt.offset, tt.value)
			assert.Equal(t, tt.expectErr, err)
			assert.Equal(t, tt.want, p.Buf)
		})
	}
}

func TestSetInt32_continuous(t *testing.T) {
	p := NewPage(16)

	for i := 0; i < 4; i++ {
		err := p.SetInt32(i*4, int32(i+1))
		assert.NoError(t, err)
	}
	assert.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04,
	}, p.Buf)
}

func TestGetBytes(t *testing.T) {
	tests := []struct {
		name      string
		pageData  []byte
		offset    int
		cursor    int
		want      []byte
		expectErr error
	}{
		{
			name:      "valid get",
			pageData:  []byte{0x00, 0x00, 0x00, 0x04, 0x01, 0x02, 0x03, 0x04},
			offset:    0,
			cursor:    8,
			want:      []byte{0x01, 0x02, 0x03, 0x04},
			expectErr: nil,
		},
		{
			name: "valid get with offset",
			pageData: []byte{
				0x00, 0x00, 0x00, 0x02, 0x01, 0x02,
				0x00, 0x00, 0x00, 0x02, 0x03, 0x04,
			},
			offset:    6,
			cursor:    12,
			want:      []byte{0x03, 0x04},
			expectErr: nil,
		},
		{
			name:      "out of bounds offset",
			pageData:  []byte{0x01, 0x02, 0x03, 0x04},
			offset:    2,
			cursor:    0,
			want:      []byte{},
			expectErr: ErrOutOfBounds,
		},
		{
			name:      "overflow data",
			pageData:  []byte{0x00, 0xff, 0xff, 0xff, 0x01},
			offset:    0,
			cursor:    0,
			want:      []byte{},
			expectErr: ErrOutOfBounds,
		},
		{
			name:      "negative offset",
			pageData:  []byte{0x01, 0x02, 0x03, 0x04},
			offset:    -1,
			cursor:    0,
			want:      []byte{},
			expectErr: ErrOutOfBounds,
		},
		{
			name:      "negative length",
			pageData:  []byte{0xff, 0xff, 0xff, 0xff, 0x01},
			offset:    0,
			cursor:    0,
			want:      []byte{},
			expectErr: ErrOutOfBounds,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Page{Buf: tt.pageData}
			got, err := p.GetBytes(tt.offset)
			assert.Equal(t, tt.expectErr, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSetBytes(t *testing.T) {
	tests := []struct {
		name      string
		pagesize  int
		offset    int
		input     []byte
		cursor    int
		want      []byte
		expectErr error
	}{
		{
			name:      "valid set",
			pagesize:  8,
			offset:    0,
			input:     []byte{0x01, 0x02, 0x03},
			cursor:    7,
			want:      []byte{0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03, 0x00},
			expectErr: nil,
		},
		{
			name:      "valid set with offset",
			pagesize:  12,
			offset:    4,
			input:     []byte{0x01, 0x02, 0x03},
			cursor:    11,
			want:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03, 0x00},
			expectErr: nil,
		},
		{
			name:      "buffer too small",
			pagesize:  2,
			offset:    1,
			input:     []byte{0x01, 0x02, 0x03},
			cursor:    0,
			want:      []byte{0x00, 0x00},
			expectErr: ErrOutOfBounds,
		},
		{
			name:      "negative offset",
			pagesize:  10,
			offset:    -1,
			input:     []byte{0x01, 0x02, 0x03},
			cursor:    0,
			want:      []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectErr: ErrOutOfBounds,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPage(tt.pagesize)
			err := p.SetBytes(tt.offset, tt.input)
			assert.Equal(t, tt.expectErr, err)
			assert.Equal(t, tt.want, p.Buf)
		})
	}
}

func TestSetBytes_continuous(t *testing.T) {
	p := NewPage(16)

	for i := 0; i < 2; i++ {
		err := p.SetBytes(i*8, []byte("hoge"))
		assert.NoError(t, err)
	}
	assert.Equal(t, []byte{
		0x00, 0x00, 0x00, 0x04, 0x68, 0x6f, 0x67, 0x65,
		0x00, 0x00, 0x00, 0x04, 0x68, 0x6f, 0x67, 0x65,
	}, p.Buf)
}
