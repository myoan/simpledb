package main

import "fmt"

type Block struct {
	Filename string
	Num      int
}

// NewBlock creates a new Block
func NewBlock(filename string, num int) *Block {
	return &Block{
		Filename: filename,
		Num:      num,
	}
}

// Equals returns true if the block is equal to the other block
func (b *Block) Equals(other *Block) bool {
	return b.Filename == other.Filename && b.Num == other.Num
}

// ToString returns a string representation of the block
func (b *Block) ToString() string {
	return fmt.Sprintf("%s:%d", b.Filename, b.Num)
}

// Hashcode returns a hash code for the block
func (b *Block) Hashcode() int {
	hash := 0
	str := b.ToString()
	for i := 0; i < len(str); i++ {
		hash = 31*hash + int(str[i])
	}
	return hash
}
