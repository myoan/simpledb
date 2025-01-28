package main

import (
	"encoding/binary"
	"errors"
)

var ErrOutOfBounds = errors.New("out of bounds")

type Page struct {
	buf []byte
}

// NewPage returns a pointer to a new Page struct
func NewPage(size int) *Page {
	return &Page{
		buf: make([]byte, size),
	}
}

// GetInt32 gets the integer at the given offset
func (p *Page) GetInt32(offset int) (int32, error) {
	if offset < 0 || offset+4 > len(p.buf) {
		return 0, ErrOutOfBounds
	}
	raw := binary.BigEndian.Uint32(p.buf[offset : offset+4])
	return int32(raw), nil
}

// SetInt32 sets the integer at the given offset
func (p *Page) SetInt32(offset int, n int32) error {
	if len(p.buf) < offset+4 {
		return ErrOutOfBounds
	}
	binary.BigEndian.PutUint32(p.buf[offset:offset+4], uint32(n))
	return nil
}

func (p *Page) GetBytes(offset int) ([]byte, error) {
	if offset < 0 || len(p.buf) < offset+4 {
		return []byte{}, ErrOutOfBounds
	}

	// Get the length of the bytes
	datalen, err := p.GetInt32(offset)
	if err != nil {
		return []byte{}, err
	}

	dstlen := offset + 4 + int(datalen)

	if datalen < 0 || dstlen > len(p.buf) {
		return []byte{}, ErrOutOfBounds
	}

	return p.buf[offset+4 : dstlen], nil
}

func (p *Page) SetBytes(offset int, b []byte) error {
	if offset < 0 || len(p.buf) < len(b) {
		return ErrOutOfBounds
	}
	err := p.SetInt32(offset, int32(len(b)))
	if err != nil {
		return err
	}
	copy(p.buf[offset+4:], b)
	return err
}

func (p *Page) GetString(offset int) (string, error) {
	data, err := p.GetBytes(offset)
	return string(data), err
}

func (p *Page) SetString(offset int, str string) error {
	return p.SetBytes(offset, []byte(str))
}

func MaxLen(strlen int) int {
	return 0
}
