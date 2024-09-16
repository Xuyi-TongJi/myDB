package dataManager

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"log"
	"reflect"
	"sync"
)

// Page

type Page interface {
	PoolObj
	Lock()
	Unlock()
	Append(toAdd []byte) error              // 插入数据
	Update(toUp []byte, offset int64) error // 更新数据
	CheckInitVersion() bool
	InitVersion()
	UpdateVersion()
	GetUsed() int64
	SetUsed(used int32)
	GetFree() int64
	GetPageType() PageType
	IsMetaPage() bool
	IsDataPage() bool
}

type PageType int32

// META page pageType & (1 << 0) == 1 else ordinary page
const (
	DbMetaPage    PageType = 1<<0 | 1<<15
	TableMetaPage PageType = 1<<0 | 1<<16
	IndexPage     PageType = 1<<1 | 1<<17
	RecordPage    PageType = 1<<1 | 1<<18
	DataPage      PageType = 1 << 1
	MetaPage      PageType = 1 << 0

	VcOn     = 100
	VcOffset = 8
	VcOff    = VcOn + VcOffset

	PageSize    int64 = 8192 // 8K bytes
	SzPgUsed    int64 = 4
	SzPageType  int64 = 4
	MaxFreeSize       = PageSize - SzPgUsed - SzPageType // 数据页面的最大使用空间
	InitOffset        = SzPgUsed + SzPageType
)

type PageImpl struct {
	lock   sync.RWMutex
	data   []byte
	dirty  bool
	pageId int64
	pc     PageCache // 每个Page组合一个PageCache，可以在操作页面时对页面缓存进行操作
}

// Page结构 [Used Space]4[Page Type]4[Data...]

func (p *PageImpl) Lock() {
	p.lock.Lock()
}

func (p *PageImpl) Unlock() {
	p.lock.Unlock()
}

func (p *PageImpl) IsDirty() bool {
	return p.dirty
}

func (p *PageImpl) SetDirty(dirty bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.dirty = dirty
}

func (p *PageImpl) GetId() int64 {
	return p.pageId
}

func (p *PageImpl) GetData() []byte {
	return p.data
}

func (p *PageImpl) GetOffset() int64 {
	return (p.pageId - 1) * PageSize
}

func (p *PageImpl) GetDataSize() int64 {
	return PageSize
}

func (p *PageImpl) SetData(data []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.data = data
}

// 数据库元数据页管理

// CheckInitVersion
// 启动检查，检查进程上次退出是否是意外退出
// 如果是意外退出，则上层需要执行恢复数据的逻辑
func (p *PageImpl) CheckInitVersion() bool {
	log.Printf("dfsfasdfsladflsdkfjasljdkfs,  %d\n", p.GetPageType())
	if p.GetPageType() != DbMetaPage {
		panic("Invalid page type when executing version checking\n")
	}
	data := p.GetData()
	v1, v2 := data[VcOn:VcOn+VcOffset], data[VcOff:VcOff+VcOffset]
	return reflect.DeepEqual(v1, v2)
}

// InitVersion 初始化版本号, 仅当系统启动时调用
func (p *PageImpl) InitVersion() {
	if p.GetPageType() != DbMetaPage {
		panic("Invalid page type when executing version checking\n")
	}
	data := p.GetData()
	if _, err := rand.Read(data[VcOn : VcOn+VcOffset]); err != nil {
		panic("Error happen when initializing version\n")
	}
}

// UpdateVersion 更新包版本号, 仅当系统正常退出时调用
func (p *PageImpl) UpdateVersion() {
	if p.GetPageType() != DbMetaPage {
		panic("Invalid page type when executing version checking\n")
	}
	data := p.GetData()
	copy(data[VcOff:VcOff+VcOffset], data[VcOn:VcOn+VcOffset])
}

// 普通页管理
// 首2个字节代表本页空闲位置偏移, 其余所有空间用于存储数据

type ErrorPageOverFlow struct{}

func (e *ErrorPageOverFlow) Error() string {
	return "Page Space overflow"
}

// Append 向页末尾添加数据
func (p *PageImpl) Append(toAdd []byte) error {
	p.Lock()
	defer p.Unlock()
	used, length := p.GetUsed(), int64(len(toAdd))
	if length+used > PageSize {
		return &ErrorPageOverFlow{}
	}
	copy(p.GetData()[used:used+length], toAdd)
	p.SetUsed(int32(used + length))
	p.SetDirty(true)
	return nil
}

// Update 更新数据页的数据
func (p *PageImpl) Update(toUp []byte, offset int64) error {
	p.Lock()
	defer p.Unlock()
	length := int64(len(toUp))
	if length+offset > PageSize {
		return &ErrorPageOverFlow{}
	}
	copy(p.GetData()[offset:offset+length], toUp)
	currentLength := p.GetUsed()
	if length+offset > currentLength {
		p.SetUsed(int32(length + offset))
	}
	p.SetDirty(true)
	return nil
}

func (p *PageImpl) GetUsed() int64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	buf := p.GetData()[:SzPgUsed]
	return int64(binary.LittleEndian.Uint32(buf))
}

func (p *PageImpl) SetUsed(used int32) {
	p.lock.Lock()
	defer p.lock.Unlock()
	buf := bytes.NewBuffer([]byte{})
	_ = binary.Write(buf, binary.LittleEndian, used)
	copy(p.GetData()[:SzPgUsed], buf.Bytes())
}

func (p *PageImpl) GetFree() int64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return PageSize - p.GetUsed()
}

func (p *PageImpl) GetPageType() PageType {
	buf := p.GetData()[SzPgUsed : SzPgUsed+SzPageType]
	return PageType(binary.LittleEndian.Uint32(buf))
}

func (p *PageImpl) IsMetaPage() bool {
	return p.GetPageType()&(1<<0) == 1
}

func (p *PageImpl) IsDataPage() bool {
	return p.GetPageType()&(1<<1) == 1
}
