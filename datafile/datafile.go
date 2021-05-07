package datafile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nireo/bitcask/encoder"
	"github.com/nireo/bitcask/hint"
	"github.com/nireo/bitcask/keydir"
)

var (
	ErrWrongByteCount = errors.New("wrote wrong amount of bytes to file.")
)

type DatafileManager struct {
	datafiles map[uint32]*Datafile
	sync.RWMutex
}

type Datafile struct {
	file *os.File
	id   uint32 // id is the unix timestamp in uint32 form

	// we need this such that we can easily create key metadata.
	offset int64

	hintFile *hint.HintFile
}

// NewDatafile creates a new datafile into a given directory. It also creates a fileid
// that is the current unix timestamp.
func NewDatafile(directory string) (*Datafile, error) {
	timestamp := uint32(time.Now().Unix())
	path := filepath.Join(directory, fmt.Sprintf("%d.df", timestamp))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	hintFile, err := hint.NewHintFile(directory, timestamp)
	if err != nil {
		return nil, err
	}

	return &Datafile{
		offset:   0,
		id:       timestamp,
		file:     f,
		hintFile: hintFile,
	}, nil
}

// readOffset reads valueSize amount of bytes starting from offset in the datafile.
func (df *Datafile) ReadOffset(offset int64, valueSize uint32) ([]byte, error) {
	// create a buffer of size valueSize and read that data starting from 'offset'
	buffer := make([]byte, valueSize)

	if df.file == nil {
		return nil, errors.New("the datafile is not set")
	}

	if _, err := df.file.Seek(offset, 0); err != nil {
		return nil, err
	}

	if _, err := df.file.Read(buffer); err != nil {
		return nil, err
	}

	return buffer, nil
}

// write writes a key-value pair in to a datafile. It also returns key-metadata such that it is
// easier to then append this key into the key-dir.
func (df *Datafile) Write(key, value []byte) (*keydir.MemEntry, error) {
	// construct the entry data
	timestamp := uint32(time.Now().Unix())
	asBytes := encoder.EncodeEntry(
		key, value, timestamp,
	)

	nBytes, err := df.file.Write(asBytes)
	if err != nil {
		return nil, err
	}

	sz := len(asBytes)
	if sz != nBytes {
		return nil, ErrWrongByteCount
	}

	if err := df.hintFile.Append(timestamp, uint32(len(value)), df.offset, key); err != nil {
		return nil, err
	}

	// now that we have stored the value offset we can add to it
	df.offset += int64(sz)

	return &keydir.MemEntry{
		Timestamp: timestamp,
		ValOffset: df.offset,
		ValSize:   uint32(len(value)),
		FileID:    df.id,
	}, nil
}

func (df *Datafile) Close() {
	df.file.Close()
	df.hintFile.Close()
}

// Offset returns offset to the end of the file.
func (df *Datafile) Offset() int64 {
	return df.offset
}

func (df *Datafile) ID() uint32 {
	return df.id
}
