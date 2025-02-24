package disk

import (
	"encoding/binary"
	"errors"
)

var ErrOutOfBounds = errors.New("out of bounds")

type Page struct {
	Buf    []byte
	cursor int
}

// NewPage returns Page struct
func NewPage(size int) *Page {
	return &Page{
		Buf:    make([]byte, size),
		cursor: 0,
	}
}

// NewPageFromBytes returns Page struct from a byte slice
func NewPageFromBytes(b []byte) *Page {
	return &Page{
		Buf:    b,
		cursor: 0,
	}
}

// GetInt32 gets the integer at the given offset
func (p *Page) GetInt32(offset int) (int32, error) {
	head := p.cursor + offset
	if offset < 0 || len(p.Buf) < head+4 {
		return 0, ErrOutOfBounds
	}
	raw := binary.BigEndian.Uint32(p.Buf[head : head+4])
	return int32(raw), nil
}

// SetInt32 sets the integer at the given offset
func (p *Page) SetInt32(offset int, n int32) error {
	head := p.cursor + offset
	if offset < 0 || len(p.Buf) < head+4 {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint32(p.Buf[head:head+4], uint32(n))
	return nil
}

// GetBytes gets the bytes at the given offset
func (p *Page) GetBytes(offset int) ([]byte, error) {
	head := p.cursor + offset
	if offset < 0 || len(p.Buf) < head+4 {
		return []byte{}, ErrOutOfBounds
	}

	datalen := binary.BigEndian.Uint32(p.Buf[offset : offset+4])
	head += 4

	if int(datalen) < 0 || len(p.Buf) < head+int(datalen) {
		return []byte{}, ErrOutOfBounds
	}

	return p.Buf[head : head+int(datalen)], nil
}

// SetBytes sets the bytes at the given offset
func (p *Page) SetBytes(offset int, b []byte) error {
	head := p.cursor + offset
	if offset < 0 || len(p.Buf) < head+4+len(b) {
		return ErrOutOfBounds
	}

	binary.BigEndian.PutUint32(p.Buf[head:head+4], uint32(len(b)))
	head += 4

	copy(p.Buf[head:], b)
	return nil
}

// GetString gets the string at the given offset
func (p *Page) GetString(offset int) (string, error) {
	data, err := p.GetBytes(offset)
	return string(data), err
}

// SetString sets the string at the given offset
func (p *Page) SetString(offset int, str string) error {
	return p.SetBytes(offset, []byte(str))
}

// MaxLen returns the maximum length of a string
func (p *Page) MaxLen(strlen int) int {
	// TODO: Consider multi-byte characters
	return strlen
}

func (p *Page) Dump() []byte {
	return p.Buf
}
