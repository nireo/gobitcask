package keydir

import "sync"

type MemEntry struct {
	FileID    uint32
	ValOffset int64
	ValSize   uint32
	Timestamp uint32
}

var keyDirLock = &sync.RWMutex{}

type KeyDir struct {
	entries map[string]*MemEntry
}

// NewKeyDir creates a new instance of a key directory.
func NewKeyDir() *KeyDir {
	return &KeyDir{
		entries: make(map[string]*MemEntry),
	}
}

// Get gets key metadata with a given key.
func (kd *KeyDir) Get(key string) *MemEntry {
	keyDirLock.RLock()
	defer keyDirLock.RUnlock()

	entry, _ := kd.entries[key]
	return entry
}

// Put adds a key with some metadata into the key directory.
func (kd *KeyDir) Put(key string, data *MemEntry) {
	keyDirLock.Lock()
	defer keyDirLock.Unlock()

	kd.entries[key] = data
}

// Delete removes the key metadata from the key directory
func (kd *KeyDir) Delete(key string) {
	keyDirLock.Lock()
	defer keyDirLock.Unlock()

	delete(kd.entries, key)
}
