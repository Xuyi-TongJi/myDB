package dataManager

import (
	"bytes"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

var (
	defaultPageFactory pageFactory
)

func init() {
	defaultPageFactory = pageFactoryImpl{}
}

// PageCache
// 基于页面Page的缓存接口

type PageCache interface {
	NewPage(pageType PageType) int64
	GetPage(pageId int64) (Page, error)
	ReleasePage(page Page) error
	SetDsSize(maxPageNumbers int64) error
	GetPageNumbers() int64
	Close()
	DoFlush(page Page) // 直接刷新到数据源
}

// Implementation
// PageCache实现类
// 实现PageCache接口
// 桥接模式

type PageCacheImpl struct {
	// implemented by PageCache
	pool        BufferPool
	ds          DataSource   // only used for NewPage-> doFlush method
	lock        *sync.Mutex  // protect the NewPage/ GetPage/ ReleasePage, the only global lock of the page cache system
	pageNumbers atomic.Int64 // the total page numbers in the DS
}

func (p *PageCacheImpl) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if err := p.pool.Close(); err != nil {
		panic(err)
	}
	if err := p.ds.Close(); err != nil {
		panic(err)
	}
}

// NewPage 新建一个页，并写入数据源
// Create a new Page and write data, then return the pageId
func (p *PageCacheImpl) NewPage(pt PageType) int64 {
	p.lock.Lock()
	defer p.lock.Unlock()
	newPage := defaultPageFactory.newPage(p.ds, p.pageNumbers.Load()+1, p, pt)
	p.pageNumbers.Add(1)
	p.DoFlush(newPage)
	return p.pageNumbers.Load()
}

// GetPage 缓存未命中时的页面获取策略
// 并发安全由BufferPool实现
// 将数据源中的数据封装成Page
func (p *PageCacheImpl) GetPage(pageId int64) (Page, error) {
	if !p.checkKeyValid(pageId) {
		panic("Invalid page id\n")
	}
	// 组装空Page
	page := defaultPageFactory.newPage(p.ds, pageId, p, -1)
	if result, err := p.pool.Get(page); err != nil {
		return nil, err
	} else {
		pg := result.(Page)
		return pg, nil
	}
}

// ReleasePage 释放对Page的引用，用于内存淘汰
func (p *PageCacheImpl) ReleasePage(page Page) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.pool.Release(page)
}

// SetDsSize 当且仅当DS没有预留maxPageNumbers个size时,申请DS预留maxPageNumbers个页的空间
func (p *PageCacheImpl) SetDsSize(maxPageNumbers int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.pageNumbers.Load() >= maxPageNumbers {
		return nil
	}
	p.pageNumbers.Store(maxPageNumbers)
	return nil
}

func (p *PageCacheImpl) GetPageNumbers() int64 {
	return p.pageNumbers.Load()
}

// This method is only in file System
func (p *PageCacheImpl) getPageOffset(pageId int64) int64 {
	return (pageId - 1) * PageSize
}

func (p *PageCacheImpl) checkKeyValid(pageId int64) bool {
	return pageId <= p.pageNumbers.Load() && pageId > 0
}

// DoFlush
// must take the lock first(private method)
// flush the page into data source, any error will panic
// ONLY WRITE TO THE OS CACHE
func (p *PageCacheImpl) DoFlush(page Page) {
	if err := p.ds.FlushBackToDataSource(page); err != nil {
		panic(err)
	}
}

// Page Factory

type pageFactory interface {
	newPage(ds DataSource, pageId int64, pc PageCache, pageType PageType) Page
}

type pageFactoryImpl struct{}

// 工厂方法
// extensible
func (p pageFactoryImpl) newPage(ds DataSource, pageId int64, pc PageCache, pageType PageType) Page {
	switch ds.(type) {
	case *FileSystemDataSource:
		data := make([]byte, PageSize)
		buf := bytes.NewBuffer([]byte{})
		_ = binary.Write(buf, binary.LittleEndian, int32(InitOffset))
		copy(data[0:SzPgUsed], buf.Bytes())
		buf = bytes.NewBuffer([]byte{})
		_ = binary.Write(buf, binary.LittleEndian, int32(pageType))
		copy(data[SzPgUsed:SzPgUsed+SzPageType], buf.Bytes())
		return &PageImpl{
			pageId: pageId, dirty: false, pc: pc, data: data,
		}
	default:
		panic("Invalid dataSource type\n")
	}
}

func NewPageCacheRefCountFileSystemImpl(maxRecourse uint32, path string, lock *sync.Mutex) PageCache {
	this := &PageCacheImpl{lock: lock}
	ds := NewFileSystemDataSource(path, lock)
	length := ds.GetDataLength()
	this.pageNumbers.Store(length / PageSize)
	this.ds = ds
	bufferPool := NewRefCountBufferPool(maxRecourse, ds, lock)
	this.pool = bufferPool
	if this.pageNumbers.Load() < 1 {
		// set db meta page
		this.NewPage(DbMetaPage)
	}
	return this
}
