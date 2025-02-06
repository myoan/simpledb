package main

import "fmt"

type BlockID struct {
	Filename string
	Num      int
}

func NewBlockID(filename string, num int) *BlockID {
	return &BlockID{
		Filename: filename,
		Num:      num,
	}
}

func (b *BlockID) Equals(other *BlockID) bool {
	return b.Filename == other.Filename && b.Num == other.Num
}

func (b *BlockID) ToString() string {
	return fmt.Sprintf("[file %s,block %d]", b.Filename, b.Num)
}

func (b *BlockID) Hashcode() int {
	hash := 0
	str := b.ToString()
	for i := 0; i < len(str); i++ {
		hash = 31*hash + int(str[i])
	}
	return hash
}
