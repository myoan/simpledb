package main

import "fmt"

type Block struct {
	Filename string
	Num      int
}

func NewBlock(filename string, num int) *Block {
	return &Block{
		Filename: filename,
		Num:      num,
	}
}

func (b *Block) Equals(other *Block) bool {
	return b.Filename == other.Filename && b.Num == other.Num
}

func (b *Block) ToString() string {
	return fmt.Sprintf("[file %s,block %d]", b.Filename, b.Num)
}

func (b *Block) Hashcode() int {
	hash := 0
	str := b.ToString()
	for i := 0; i < len(str); i++ {
		hash = 31*hash + int(str[i])
	}
	return hash
}
