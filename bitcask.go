package bitcask

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nireo/bitcask/datafile"
	"github.com/nireo/bitcask/hint"
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
		if err := os.Mkdir(directory, 0777); err != nil {
			return nil, err
		}
	}

	if options == nil {
		options = DefaultConfigurtion()
	}

	// we want to parse the datafiles before creating another writable one

	db := &DB{
		Options:   options,
		KeyDir:    keydir.NewKeyDir(),
		rwmutex:   &sync.RWMutex{},
		directory: directory,
		Manager:   make(map[uint32]*datafile.Datafile),
	}

	if err := db.parsePersistanceFiles(); err != nil {
		return nil, err
	}

	writableFile, err := datafile.NewDatafile(directory)
	if err != nil {
		return nil, err
	}

	db.WFile = writableFile

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
	db.rwmutex.RLock()
	defer db.rwmutex.RLock()

	entry := db.KeyDir.Get(string(key))
	if entry == nil {
		return nil, errors.New("could not find value from keydir")
	}

	file, err := db.getDataFile(entry.FileID)
	if err != nil {
		return nil, errors.New("could not find key in the specified data file")
	}

	value, err := file.ReadOffset(entry.ValOffset, entry.ValSize)
	if err != nil {
		return nil, errors.New("could not find key in the specified data file")
	}

	return value, nil
}

func (db *DB) getDataFile(id uint32) (*datafile.Datafile, error) {
	if db.WFile.ID() == id {
		return db.WFile, nil
	}

	file, ok := db.Manager[id]
	if !ok {
		return nil, errors.New("could not find datafile file")
	}

	return file, nil
}

// parsePersistanceFiles takes in all of the hint files and then parses their metadata into
// the keydirectory. The hint files are used to reduce startup time since without we would have
// to scan files that are multiple gigabytes large.
func (db *DB) parsePersistanceFiles() error {
	var hintfiles []string

	files, err := ioutil.ReadDir(db.directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".hnt") {
			hintfiles = append(hintfiles, file.Name())
		}

		if strings.HasSuffix(file.Name(), ".df") {
			df, err := datafile.NewReadOnlyDatafile(filepath.Join(
				db.directory, file.Name(),
			))
			if err != nil {
				return err
			}

			db.Manager[df.ID()] = df
		}
	}

	for _, hintfile := range hintfiles {
		fileID, err := datafile.ParseID(hintfile)
		if err != nil {
			log.Printf("could not parse file: %d", fileID)
			continue
		}

		if err := hint.AppendPathToKeyDir(
			filepath.Join(db.directory, hintfile),
			fileID,
			db.KeyDir,
		); err != nil {
			log.Printf("could not parse file: %d", fileID)
			continue
		}
	}

	return nil
}
