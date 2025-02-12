package main

type SimpleDB struct {
	BufferManager *BufferManager
	fm            *FileManager
	lm            *LogManager
}

func NewDB(filename string, blocksize, bufsize int) (*SimpleDB, error) {
	fm := NewFileManager(blocksize)
	lm, err := NewLogManager(fm, filename+".log")
	if err != nil {
		return nil, err
	}
	bm := NewBufferManager(fm, lm, bufsize)
	return &SimpleDB{
		fm:            fm,
		lm:            lm,
		BufferManager: bm,
	}, nil
}
