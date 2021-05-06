package bitcask

import (
	"errors"
	"os"
	"sync"

	"github.com/nireo/bitcask/datafile"
	"github.com/nireo/bitcask/keydir"
)

const (
	// MaxDatafileSize is 512 mb by default.
	MaxDatafileSize int64 = 512 * 1024 * 1024
)

type Options struct {
	MaxDatafileSize int64
}

func DefaultConfigurtion() *Options {
	return &Options{
		MaxDatafileSize: MaxDatafileSize,
	}
}

type DB struct {
	Options   *Options
	KeyDir    *keydir.KeyDir
	directory string

	// mapping the file ids into the datafiles.
	Manager map[uint32]*datafile.Datafile
	WFile   *datafile.Datafile
	rwmutex *sync.RWMutex
}

// GetDirectory returns the directory in which all the datafiles are begin stored.
func (db *DB) GetDirectory() string {
	return db.directory
}

// Open starts the database from a directory
func Open(directory string, options *Options) (*DB, error) {
	// check if the directory exists
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		// it doesnt exist
		if err := os.Mkdir(directory, os.ModeDir); err != nil {
			return nil, err
		}
	}

	if options == nil {
		options = DefaultConfigurtion()
	}

	db := &DB{
		Options: options,
	}

	return db, nil
}

// Put places a key-value pair into the database
func (db *DB) Put(key, value []byte) error {
	db.rwmutex.Lock()
	defer db.rwmutex.Unlock()

	// TODO: check the if the writable file is too large.
	if db.WFile.Offset() > db.Options.MaxDatafileSize {

	}

	entry, err := db.WFile.Write(key, value)
	if err != nil {
		return err
	}

	// write to the keydir
	db.KeyDir.Put(string(key), entry)

	return nil
}

// Close closes the database this is normally used when defering
func (db *DB) Close() {
	db.WFile.Close()
}

// Get finds value with key and then returns the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	entry := db.KeyDir.Get(string(key))
	if entry == nil {
		return nil, errors.New("could not find value from keydir")
	}

	value, err := db.Manager[entry.FileID].ReadOffset(entry.ValOffset, entry.ValSize)
	if err != nil {
		return nil, errors.New("could not find key in the specified data file")
	}

	return value, nil
}
