package keydir

import "sync"

type MemEntry struct {
	FileID    uint32
	ValOffset int64
	ValSize   uint32
	Timestamp uint32
}

var keyDirLock *sync.RWMutex

type KeyDir struct {
	entries map[string]*MemEntry
}

func (kd *KeyDir) Get(key string) *MemEntry {
	keyDirLock.RLock()
	defer keyDirLock.RUnlock()

	entry, _ := kd.entries[key]
	return entry
}

func (kd *KeyDir) Put(key string, data *MemEntry) {
	keyDirLock.Lock()
	defer keyDirLock.Unlock()

	kd.entries[key] = data
}

func (kd *KeyDir) Delete(key string) {
	keyDirLock.Lock()
	defer keyDirLock.Unlock()

	delete(kd.entries, key)
}
