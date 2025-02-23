package disk

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var ErrOutOfBounds = errors.New("out of bounds")

type Page struct {
	buf    []byte
	cursor int
}

// NewPage returns Page struct
func NewPage(size int) *Page {
	return &Page{
		buf:    make([]byte, size),
		cursor: 0,
	}
}

// NewPageFromBytes returns Page struct from a byte slice
func NewPageFromBytes(b []byte) *Page {
	return &Page{
		buf:    b,
		cursor: 0,
	}
}

func (p *Page) Buf() []byte {
	return p.buf
}

// GetInt32 gets the integer at the given offset
func (p *Page) GetInt32(offset int) (int32, error) {
	head := p.cursor + offset
	if offset < 0 || len(p.buf) < head+4 {
		return 0, ErrOutOfBounds
	}
	raw := binary.BigEndian.Uint32(p.buf[head : head+4])
	return int32(raw), nil
}

// SetInt32 sets the integer at the given offset
func (p *Page) SetInt32(offset int, n int32) error {
	head := p.cursor + offset
	if offset < 0 || len(p.buf) < head+4 {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint32(p.buf[head:head+4], uint32(n))
	return nil
}

// GetBytes gets the bytes at the given offset
func (p *Page) GetBytes(offset int) ([]byte, error) {
	head := p.cursor + offset
	if offset < 0 || len(p.buf) < head+4 {
		return []byte{}, ErrOutOfBounds
	}
	fmt.Printf("head: %d, data: %v\n", head, p.buf)

	datalen := binary.BigEndian.Uint32(p.buf[offset : offset+4])
	head += 4

	if int(datalen) < 0 || len(p.buf) < head+int(datalen) {
		return []byte{}, ErrOutOfBounds
	}

	fmt.Printf("head: %d, len: %d, data: %v\n", head, datalen, p.buf)
	fmt.Printf("data: %v\n", p.buf[head:head+int(datalen)])
	return p.buf[head : head+int(datalen)], nil
}

// SetBytes sets the bytes at the given offset
func (p *Page) SetBytes(offset int, b []byte) error {
	head := p.cursor + offset
	if offset < 0 || len(p.buf) < head+4+len(b) {
		return ErrOutOfBounds
	}

	binary.BigEndian.PutUint32(p.buf[head:head+4], uint32(len(b)))
	head += 4

	copy(p.buf[head:], b)
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
	return p.buf
}
