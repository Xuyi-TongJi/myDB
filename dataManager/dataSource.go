package dataManager

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
)

type DataSource interface {
	GetFromDataSource(obj PoolObj) ([]byte, error)
	FlushBackToDataSource(obj PoolObj) error
	Truncate(size int64) error
	Close() error
	GetDataLength() int64
}

// FileSystemDataSource

type FileSystemDataSource struct {
	file *os.File
	lock *sync.Mutex
}

const (
	FileSuffix string = ".fds"
)

type FileSystemObj interface {
	PoolObj
	GetOffset() int64
}

func NewFileSystemDataSource(path string, lock *sync.Mutex) DataSource {
	f, err := os.OpenFile(path+FileSuffix, os.O_RDWR, 0666)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			f, err = os.Create(path + FileSuffix)
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
	log.Printf("[Data Manager] Open source file\n")
	fsd := &FileSystemDataSource{
		file: f,
		lock: lock,
	}
	return fsd
}

// GetFromDataSource
// 缓存未命中的执行逻辑
// 从文件系统中读取, 返回读到的字节数组
// param: offset, size
func (ch *FileSystemDataSource) GetFromDataSource(obj PoolObj) ([]byte, error) {
	fso, ok := obj.(FileSystemObj)
	if !ok {
		panic("File System Data Source illegal param\n")
	}
	offset, size := fso.GetOffset(), fso.GetDataSize()
	buf := make([]byte, size)
	_, err := ch.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// FlushBackToDataSource
// 缓存淘汰时写回磁盘
// param: offset, size, data
func (ch *FileSystemDataSource) FlushBackToDataSource(obj PoolObj) error {
	fso, ok := obj.(FileSystemObj)
	if !ok {
		panic("File System Data Source illegal param\n")
	}
	_, err := ch.file.WriteAt(fso.GetData(), fso.GetOffset())
	return err
}

func (ch *FileSystemDataSource) Truncate(size int64) error {
	return ch.file.Truncate(size)
}

func (ch *FileSystemDataSource) Close() error {
	return ch.file.Close()
}

func (ch *FileSystemDataSource) GetDataLength() int64 {
	stat, _ := ch.file.Stat()
	return stat.Size()
}

// Debug only for debugging
func (ch *FileSystemDataSource) Debug() {
	stat, _ := ch.file.Stat()
	fmt.Println(stat.Size())
}
