package dataManager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"myDB/transactions"
	"os"
	"sync"
)

// redo log实现
// 功能：Crash Recovery
// 迭代器模式
// 所有操作必须先记录日志（保证刷入磁盘后），再执行数据操作
// Any error will panic

type Log interface {
	UpdateLog(uid, xid int64, oldRaw, raw []byte)
	InsertLog(uid, xid int64, raw []byte)
	log(data []byte) // 记录下一条log
	Close()
	Next() []byte // 迭代器获得下一条log data
	ResetLog()
	CrashRecover(pc PageCache, tm transactions.TransactionManager) // 崩溃恢复
}

const (
	SEED       int64  = 1331
	MOD        int64  = 998244353
	LogSuffix  string = "_redo.log"
	SzCheckSum int64  = 8
	SzData     int64  = 4
)

type RedoLog struct {
	file     *os.File
	checkSum int64
	lock     *sync.Mutex
	offset   int64 // current pointer used for iterator
}

func (redo *RedoLog) UpdateLog(uid, xid int64, oldRaw, raw []byte) {
	pageId, offset := uidTrans(uid)
	updateLog := wrapUpdateLog(xid, pageId, offset, int64(len(oldRaw)), oldRaw, raw)
	redo.log(updateLog)
}

func (redo *RedoLog) InsertLog(uid, xid int64, raw []byte) {
	pageId, offset := uidTrans(uid)
	// Insert 本质 INVALID -> VALID
	oldRaw := SetRawInvalid(raw)
	insertLog := wrapUpdateLog(xid, pageId, offset, int64(len(oldRaw)), oldRaw, raw)
	redo.log(insertLog)
}

// log
// [Size]4[CheckSum]8[Data] -> log raw format
// Must flush the wrapped data and then update the checkSum of the redo log file
// 先写log,最后更新checkSum
func (redo *RedoLog) log(data []byte) {
	redo.lock.Lock()
	defer redo.lock.Unlock()
	logWrap := wrapLog(data)
	// write
	if _, err := redo.file.Write(logWrap); err != nil {
		panic(fmt.Sprintf("Error occurs when writing redo log, err = %s", err))
	}
	// finally update the checkSum
	nextCheckSum := calcCheckSum(redo.checkSum, data)
	buffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer, binary.BigEndian, nextCheckSum)
	if _, err := redo.file.WriteAt(buffer.Bytes(), 0); err != nil {
		panic(fmt.Sprintf("Error occurs when writing redo log, err = %s", err))
	}
	log.Printf("[Data Manager] Log a new redo log, current checkSum = %d\n", nextCheckSum)
	redo.checkSum = nextCheckSum
}

func (redo *RedoLog) Close() {
	redo.lock.Lock()
	defer redo.lock.Unlock()
	if err := redo.file.Close(); err != nil {
		panic(err)
	}
}

// Next 迭代器模式
// nextUnlock 的加锁实现
func (redo *RedoLog) Next() []byte {
	redo.lock.Lock()
	defer redo.lock.Unlock()
	return redo.nextUnlock()
}

func (redo *RedoLog) reset() {
	redo.offset = SzCheckSum
}

func (redo *RedoLog) ResetLog() {
	if err := redo.file.Truncate(0); err != nil {
		panic(fmt.Sprintf("Error occurs when reseting redo log, err : %s\n", err))
	}
	buffer := bytes.NewBuffer(make([]byte, 0))
	_ = binary.Write(buffer, binary.BigEndian, int32(0))
	// 4 bytes checkSum
	if _, err := redo.file.Write(buffer.Bytes()); err != nil {
		panic(fmt.Sprintf("Error occurs when reseting redo log, err : %s\n", err))
	}
}

// init
// 根据redoLog文件恢复现场参数(offset, checkSum)
// 主要逻辑：removeTail 去除上次崩溃时还未写完的tail
func (redo *RedoLog) init() {
	// read checkSum
	stat, _ := redo.file.Stat()
	if stat.Size() < SzCheckSum {
		panic("Invalid checkSum length when initializing redo log\n")
	}
	buf := make([]byte, SzCheckSum)
	if _, err := redo.file.ReadAt(buf, 0); err != nil {
		panic(fmt.Sprintf("Error occuring when initializing redo log, %s\n", err))
	}
	redo.checkSum = int64(binary.BigEndian.Uint64(buf))
	redo.removeTail() // set offset
}

// removeTail
// 移除log中上次关闭未写完的部分
// only in init method
func (redo *RedoLog) removeTail() {
	redo.reset()
	var checkedCheckSum int64 = 0
	for true {
		nextLogData := redo.nextUnlock()
		if nextLogData == nil {
			break
		}
		checkedCheckSum = calcCheckSum(checkedCheckSum, nextLogData)
	}
	if checkedCheckSum != redo.checkSum {
		panic("Invalid redo log file\n")
	}
	// truncate
	redo.truncate(redo.offset)
	redo.reset() // roll back the pointer
}

// truncate 截断文件
func (redo *RedoLog) truncate(size int64) {
	if err := redo.file.Truncate(size); err != nil {
		panic(err)
	}
}

// nextUnlock
// 仅适用于 removeTail / CrashRecover 方法
// return the data of next log
// if !hasNext or the next log is invalid then return nil
func (redo *RedoLog) nextUnlock() (data []byte) {
	// next log is invalid
	stat, _ := redo.file.Stat()
	totSize := stat.Size()
	if redo.offset+SzData+SzCheckSum > totSize {
		return nil
	}
	buffer := make([]byte, SzData)
	if _, err := redo.file.ReadAt(buffer, redo.offset); err != nil {
		panic(err)
	}
	dataSize := int64(binary.BigEndian.Uint32(buffer))
	if redo.offset+SzData+SzCheckSum+dataSize > totSize {
		return nil
	}
	data = make([]byte, dataSize)
	if _, err := redo.file.ReadAt(data, redo.offset+SzData+SzCheckSum); err != nil {
		panic(err)
	}
	// 当且仅当完整读完一条log时，更改offset
	redo.offset += SzData + SzCheckSum + dataSize
	return
}

// Crash Recovery
// This Crash Recovery can only run under the isolation level of "READ REPEATABLE"
// 必须保证进入DataManager记录数据的操作满足RR以上隔离级别，否则恢复系统将失效

// or, this recovery mechanism will be invalid
// Data format of LOG RAW [Size]4[CheckSum]8[Data]
// Data format of updateLog [LogType]4[XID]8[PageId]8[Offset]8[OldRawLength]8[OldRaw][NewRaw]
// Data format of insertLog [LogType]4[XID]8[PageId]8[Offset]8[Raw]
// XID -> transaction id XID must also be updated first before updating the data

type OperationType int32

type RecoveryType int32

type TransactionMap map[int64][][]byte

func NewTransactionMap() TransactionMap {
	return make(map[int64][][]byte, 0)
}

const (
	UPDATE   OperationType = 0 // INSERT and DELETE is essentially a UPDATE operation
	INSERT   OperationType = 1 // unnecessary
	SzOpt    int           = 4
	SzXid    int           = 8
	SzPageId int           = 8
	SzOffset int           = 8
	REDO     RecoveryType  = 0
	UNDO     RecoveryType  = 1
)

// CrashRecover
// do crash recover only when the storage engine starts
// no lock
// undo all the transaction if not finished
// redo all the transaction if finished
func (redo *RedoLog) CrashRecover(pc PageCache, tm transactions.TransactionManager) {
	log.Printf("Recoving Data...\n")
	// remove Tail
	redo.init()
	toRedo, toUndo := NewTransactionMap(), NewTransactionMap()
	redo.reset()
	var maxPageId int64 = 1
	for true {
		nextLog := redo.nextUnlock()
		if nextLog == nil {
			break
		}
		xid := getXid(nextLog)
		pageId := getPageId(nextLog)
		xStatus := tm.Status(xid)
		if xStatus&(1<<transactions.FINISH) == 0 {
			// undo 撤销
			toUndo[xid] = append(toUndo[xid], nextLog)
		} else {
			// redo 重做
			toRedo[xid] = append(toRedo[xid], nextLog)
		}
		if pageId > maxPageId {
			maxPageId = pageId
		}
	}
	// set ds size if needed
	if err := pc.SetDsSize(maxPageId); err != nil {
		panic("Error occurs when truncating page cache\n")
	}
	log.Printf("Recovering redo\n")
	redoRecovery(toRedo, pc)
	log.Printf("Recovering undo\n")
	undoRecovery(toUndo, pc, tm)
	log.Printf("Recovery finish\n")
}

// redo
// 对所有完成的事物(FINISH)进行正序重新执行
func redoRecovery(tx TransactionMap, pc PageCache) {
	for _, logs := range tx {
		for _, lg := range logs {
			opt := getOperationType(lg)
			if opt == UPDATE {
				doUpdateRecovery(lg, pc, REDO)
			}
		}
	}
}

// undo
// 对所有崩溃时未完成的事物(ACTIVE)进行倒序回滚
func undoRecovery(tx TransactionMap, pc PageCache, tm transactions.TransactionManager) {
	for xid, logs := range tx {
		length := len(logs)
		for i := length - 1; i >= 0; i-- {
			opt := getOperationType(logs[i])
			if opt == UPDATE {
				doUpdateRecovery(logs[i], pc, UNDO)
			}
		}
		// set aborted
		tm.Abort(xid)
	}
}

//// doInsertRecovery 执行插入操作
//func doInsertRecovery(data []byte, pc PageCache, opt RecoveryType) {
//	// TODO
//	_, pageId, offset, raw, oldRaw := parseInsertLog(data)
//	if pg, err := pc.GetPage(pageId); err != nil {
//		panic(fmt.Sprintf("Error occurs when getting page, err = %s\n", err))
//	} else {
//		var err error
//		if opt == REDO {
//			err = pg.Update(raw, offset)
//		} else {
//			// TODO INSERT如何撤销
//			err = pg.Remove(raw, offset)
//		}
//		if err != nil {
//			panic(fmt.Sprintf("Error occurs when recovering data, err = %s\n", err))
//		}
//		if err = pc.ReleasePage(pg); err != nil {
//			panic(fmt.Sprintf("Error occurs when releasing page, err = %s\n", err))
//		}
//	}
//}

// doUpdateRecovery 执行更新恢复操作
func doUpdateRecovery(data []byte, pc PageCache, opt RecoveryType) {
	_, pageId, offset, _, oldRaw, newRaw := parseUpdateLog(data)
	if pg, err := pc.GetPage(pageId); err != nil {
		panic(fmt.Sprintf("Error occurs when getting page, err = %s\n", err))
	} else {
		var err error
		if opt == REDO {
			// REDO
			err = pg.Update(newRaw, offset)
		} else {
			// UNDO
			err = pg.Update(oldRaw, offset)
		}
		if err != nil {
			panic(fmt.Sprintf("Error occurs when recoving data, err = %s\n", err))
		}
		if err = pc.ReleasePage(pg); err != nil {
			panic(fmt.Sprintf("Error occurs when releasing page, err = %s\n", err))
		}
	}
}

// Create / Init(Load) Redo Log

func CreateRedoLog(path string, lock *sync.Mutex) Log {
	file, err := os.Create(path + LogSuffix)
	if err != nil {
		panic(err)
	}
	buffer := bytes.NewBuffer(make([]byte, 0))
	_ = binary.Write(buffer, binary.BigEndian, int64(0))
	// 8 bytes checkSum
	if _, err := file.Write(buffer.Bytes()); err != nil {
		panic(err)
	}
	redoLog := &RedoLog{
		file:     file,
		checkSum: 0,
		lock:     lock,
	}
	redoLog.reset()
	return redoLog
}

func OpenRedoLog(path string, lock *sync.Mutex) Log {
	file, err := os.OpenFile(path+LogSuffix, os.O_RDWR, 0666)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return CreateRedoLog(path, lock)
	} else if err != nil {
		panic(err)
	}
	redoLog := &RedoLog{
		file: file,
		lock: lock,
	}
	//redoLog.init()
	log.Printf("[Data Manager] Open redo log\n")
	return redoLog
}

// utils

func calcCheckSum(checkSum int64, data []byte) int64 {
	for _, b := range data {
		checkSum = (checkSum + SEED*int64(b)%MOD) % MOD
	}
	return checkSum
}

// 将size...checkSum...data 包装成一条log
func wrapLog(data []byte) []byte {
	checkSum := calcCheckSum(0, data)
	size := len(data)
	buffer1 := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer1, binary.BigEndian, int32(size))
	buffer2 := bytes.NewBuffer([]byte{})
	_ = binary.Write(buffer2, binary.BigEndian, checkSum)
	// [size]4[checkSum]8[data]
	ret := make([]byte, SzCheckSum+SzData+int64(len(data)))
	copy(ret[:SzData], buffer1.Bytes())
	copy(ret[SzData:SzCheckSum+SzData], buffer2.Bytes())
	copy(ret[SzCheckSum+SzData:], data)
	return ret
}

// Operation Parser

func getOperationType(data []byte) OperationType {
	return OperationType(binary.BigEndian.Uint32(data[0:SzOpt]))
}

func getXid(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data[SzOpt : SzXid+SzOpt]))
}

func getPageId(data []byte) int64 {
	return int64(binary.BigEndian.Uint64(data[SzOpt+SzXid : SzOpt+SzXid+SzPageId]))
}

// Unnecessary
//func wrapInsertLog(xid, pageId, offset int64, raw []byte) []byte {
//	buffer := bytes.NewBuffer(make([]byte, 0))
//	_ = binary.Write(buffer, binary.BigEndian, int32(INSERT))
//	_ = binary.Write(buffer, binary.BigEndian, xid)
//	_ = binary.Write(buffer, binary.BigEndian, pageId)
//	_ = binary.Write(buffer, binary.BigEndian, offset)
//	_ = binary.Write(buffer, binary.BigEndian, raw)
//	return buffer.Bytes()
//}

//func parseInsertLog(data []byte) (xid, pageId, offset int64, raw []byte) {
//	data = data[SzOpt:]
//	xid = int64(binary.BigEndian.Uint64(data[0:SzXid]))
//	pageId = int64(binary.BigEndian.Uint64(data[SzXid : SzXid+SzPageId]))
//	offset = int64(binary.BigEndian.Uint64(data[SzXid+SzPageId : SzXid+SzPageId+SzOffset]))
//	raw = data[SzXid+SzPageId+SzOffset:]
//	return
//}

func wrapUpdateLog(xid, pageId, offset, oldRawLength int64, oldRaw, newRaw []byte) []byte {
	buffer := bytes.NewBuffer(make([]byte, 0))
	_ = binary.Write(buffer, binary.BigEndian, int32(UPDATE))
	_ = binary.Write(buffer, binary.BigEndian, xid)
	_ = binary.Write(buffer, binary.BigEndian, pageId)
	_ = binary.Write(buffer, binary.BigEndian, offset)
	_ = binary.Write(buffer, binary.BigEndian, oldRawLength)
	_ = binary.Write(buffer, binary.BigEndian, oldRaw)
	_ = binary.Write(buffer, binary.BigEndian, newRaw)
	return buffer.Bytes()
}

func parseUpdateLog(data []byte) (xid, pageId, offset, oldRawLength int64, oldRaw, newRaw []byte) {
	data = data[SzOpt:]
	xid = int64(binary.BigEndian.Uint64(data[0:SzXid]))
	pageId = int64(binary.BigEndian.Uint64(data[SzXid : SzXid+SzPageId]))
	offset = int64(binary.BigEndian.Uint64(data[SzXid+SzPageId : SzXid+SzPageId+SzOffset]))
	oldRawLength = int64(binary.BigEndian.Uint64(data[SzXid+SzPageId+SzOffset : SzXid+SzPageId+SzOffset+8]))
	oldRaw = data[SzXid+SzPageId+SzOffset+8 : SzXid+SzPageId+SzOffset+8+int(oldRawLength)]
	newRaw = data[SzXid+SzPageId+SzOffset+8+int(oldRawLength):]
	return
}
