package dataManager

import (
	"encoding/binary"
	"fmt"
	. "myDB/transactions"
	"sync"
)

// DataManager 管理PageCache(BufferPool+Data Source), Page Control, RedoLog
// 上层请求必须保证请求的长度八字节对齐

const PageNumberDbMeta int64 = 1

type DataManager interface {
	Read(uid int64) DataItem
	Update(xid, uid int64, data []byte) int64
	Insert(xid int64, data []byte) int64
	Delete(xid, uid int64)
	Release(di DataItem)
	Close()
}

type DmImpl struct {
	pageCache          PageCache
	pageCtl            PageCtl
	redo               Log
	transactionManager TransactionManager
	metaPage           Page // 数据库元数据页(直到dataManager关闭不会被换出)
}

// Read
// 根据uid从PC中读取DataItem并校验有效位
// 可能返回nil
func (dm *DmImpl) Read(uid int64) DataItem {
	pageId, offset := uidTrans(uid)
	if page, err := dm.pageCache.GetPage(pageId); err != nil {
		panic(fmt.Sprintf("Error occurs when getting pages, err = %s", err))
	} else {
		item := dm.getDataItem(page, offset)
		if item.IsValid() {
			dm.Release(item)
			return item
		} else {
			dm.Release(item)
			return nil
		}
	}
}

// Update
// 更新数据
// 尝试更新失效的或者不存在的数据时，panic
// 更新的数据长度小于，原地更新，否则将当前DataItem设置为无效，并且新插入一个DataItem
// 返回新数据的地址
// 上层模块保证其操作的安全性（VersionManager）
func (dm *DmImpl) Update(xid, uid int64, data []byte) int64 {
	di := dm.Read(uid)
	if di == nil {
		panic("Error occurs when updating data item, this data item is invalid")
	}
	oldRaw := di.GetRaw()
	newRaw := WrapDataItemRaw(data)
	var ret int64
	if len(oldRaw) >= len(newRaw) {
		// 原地更新
		// LOG FIRST
		dm.redo.UpdateLog(uid, xid, oldRaw, newRaw)
		di.Update(newRaw)
		ret = uid
	} else {
		// DELETE
		dm.Delete(xid, uid)
		// INSERT
		ret = dm.Insert(xid, data)
	}
	di.Release()
	return ret
}

// Insert
// 申请向Page Cache插入一段数据
// log first and insert next
// return uid(pageId, offset)
func (dm *DmImpl) Insert(xid int64, data []byte) int64 {
	// wrap
	raw := WrapDataItemRaw(data)
	length := int64(len(raw))
	if length > MaxFreeSize {
		// 暂不支持跨页存储
		panic("Error occurs when inserting data, err = data length overflow\n")
	}
	// find a free page by page Ctl
	var pi *PageInfo
	pi = dm.pageCtl.Select(length)
	var pageId int64
	// if necessarily, create a new page
	if pi == nil {
		pageId = dm.pageCache.NewPage(DataPage)
	} else {
		pageId = pi.PageId
	}
	pg, err := dm.pageCache.GetPage(pageId)
	if err != nil {
		panic(fmt.Sprintf("Error occurs when getting page, err = %s", err))
	}
	offset := pg.GetUsed()
	// LOG FIRST
	dm.redo.InsertLog(xid, getUid(pg.GetId(), offset), raw)
	// update page data
	if err := pg.Append(raw); err != nil {
		panic(fmt.Sprintf("Error occurs when updating page, err = %s\n", err))
	}
	// update pageCtl
	dm.pageCtl.AddPageInfo(pg.GetId(), pg.GetFree())
	// release
	if err := dm.pageCache.ReleasePage(pg); err != nil {
		panic(fmt.Sprintf("Error occurs when releasing page, err = %s\n", err))
	}
	return getUid(pg.GetId(), offset)
}

func (dm *DmImpl) Release(di DataItem) {
	if err := dm.pageCache.ReleasePage(di.GetPage()); err != nil {
		panic(err)
	}
}

// Delete
// 删除一个DataItem(set invalid)
func (dm *DmImpl) Delete(xid, uid int64) {
	di := dm.Read(uid)
	if di != nil {
		// LOG FIRST
		oldRaw := di.GetRaw()
		newRaw := make([]byte, len(oldRaw))
		copy(newRaw, oldRaw)
		SetRawInvalid(newRaw)
		dm.redo.UpdateLog(uid, xid, oldRaw, newRaw)
		di.SetInvalid()
	}
	di.Release()
}

func (dm *DmImpl) Close() {
	dm.transactionManager.Close()
	dm.redo.Close()
	dm.metaPage.UpdateVersion()
	if err := dm.pageCache.ReleasePage(dm.metaPage); err != nil {
		panic(fmt.Sprintf("Error occurs when releasing db meta page, err = %s", err))
	}
	dm.pageCache.Close()
}

func (dm *DmImpl) init() {
	if metaPage, err := dm.pageCache.GetPage(PageNumberDbMeta); err != nil {
		panic(err)
	} else {
		dm.metaPage = metaPage
	}
	// 数据恢复
	if !dm.metaPage.CheckInitVersion() {
		dm.redo.CrashRecover(dm.pageCache, dm.transactionManager)
	}
	// 重置日志文件
	dm.redo.ResetLog()
	// 初始化版本号
	dm.metaPage.InitVersion()
	dm.pageCache.DoFlush(dm.metaPage)
	dm.pageCtl.Init(dm.pageCache)
}

// getDataItem
// get DataItem from the dataManger by the page
func (dm *DmImpl) getDataItem(page Page, offset int64) DataItem {
	// start from the offset of data
	data := page.GetData()
	// RAW [valid]1[size]8[data]
	dataSize := int64(binary.BigEndian.Uint64(data[offset+SzDIValid : offset+SzDIValid+SzDIDataSize]))
	raw := data[offset : offset+SzDIValid+SzDIDataSize+dataSize]
	oldRaw := make([]byte, len(raw))
	uid := getUid(page.GetId(), offset)
	return NewDataItem(raw, oldRaw, &sync.RWMutex{}, dm, page, uid)
}

// uid 高32位为pageId, 低32位为offset
func uidTrans(uid int64) (pageId, offset int64) {
	offset = uid & ((1 << 32) - 1)
	uid >>= 32
	pageId = uid & ((1 << 32) - 1)
	return
}

func getUid(pageId, offset int64) int64 {
	return (pageId << 32) | offset
}

func OpenDataManager(path string, memory int64, tm TransactionManager) DataManager {
	pc := NewPageCacheRefCountFileSystemImpl(uint32(memory/PageSize), path, &sync.Mutex{})
	pageCtl := NewPageCtl(&sync.Mutex{}, pc)
	redo := OpenRedoLog(path, &sync.Mutex{})
	dm := &DmImpl{
		pageCache:          pc,
		pageCtl:            pageCtl,
		redo:               redo,
		transactionManager: tm,
	}
	dm.init()
	return dm
}
