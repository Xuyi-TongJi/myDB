package dataManager

// Lru TODO 基于LRU实现BufferPool

type LruBufferPool struct {
}

func (l *LruBufferPool) Get(objId int64) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LruBufferPool) Release(key PoolObj) error {
	//TODO implement me
	panic("implement me")
}

func (l *LruBufferPool) Close() error {
	//TODO implement me
	panic("implement me")
}
