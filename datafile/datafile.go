package datafile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/nireo/bitcask/encoder"
	"github.com/nireo/bitcask/hint"
	"github.com/nireo/bitcask/keydir"
)

var (
	ErrWrongByteCount = errors.New("wrote wrong amount of bytes to file.")
	ErrNoFileID       = errors.New("the filename didn't contain a fileid")
	ErrNotInManager   = errors.New("the given id was not found in the manager")
)

// DatafileManager takes care of managing read-only instances of datafiles.
type DatafileManager struct {
	datafiles map[uint32]*Datafile
	*sync.RWMutex
}

// Set simply adds a given datafile into the manager. This datafile should be in
// read-only mode. And not in write mode.
func (dfm *DatafileManager) Set(df *Datafile) {
	dfm.Lock()
	defer dfm.Unlock()

	dfm.datafiles[df.ID()] = df
}

// Delete removes a datafile from the manager
func (dfm *DatafileManager) Delete(id uint32) error {
	dfm.Lock()
	defer dfm.Unlock()

	df, ok := dfm.datafiles[id]
	if !ok {
		return ErrNotInManager
	}
	// make sure we close the file
	df.Close()

	delete(dfm.datafiles, id)
	return nil
}

// Get returns a datafile from the manager
func (dfm *DatafileManager) Get(id uint32) *Datafile {
	dfm.RLock()
	defer dfm.RUnlock()

	df, _ := dfm.datafiles[id]
	return df
}

// Datafile represents a file that contains all the information about a key-value pair. It also contains
// a hint file which contains less information about the pairs to lower start-up time.
type Datafile struct {
	file *os.File
	id   uint32 // id is the unix timestamp in uint32 form

	// we need this such that we can easily create key metadata.
	offset int64

	hintFile *hint.HintFile
}

// DatafileScanner contains a bufio.Scanner that has a certain Split method specified to properly
// go through the entries in a simple fashion.
type DatafileScanner struct {
	file   *os.File
	offset int64
	amount int
}

// Entry represents all of the data in a datafile entry excluding the CRC32 hash, since that
// is checked before creating an entry instance.
type Entry struct {
	Timestamp uint32
	KeySize   uint32
	ValueSize uint32

	Key   []byte
	Value []byte
}

func (df *Datafile) GetPath(directory string) string {
	return df.file.Name()
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

// NewReadOnlyDatafile takes in a path for a datafile and then opens a read-only pointer to that file
// This is done such the other datafiles cannot be written after the current datafile is changed.
func NewReadOnlyDatafile(path string) (*Datafile, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}

	fileID, err := ParseID(path)
	if err != nil {
		return nil, err
	}

	// no need to parse the hint file since a read-only file will not do anything
	// with the hint file pointer.
	return &Datafile{
		offset:   0,
		file:     f,
		id:       fileID,
		hintFile: nil,
	}, nil
}

// ParseID parses the last number from a given path. We take the last number since the directory in
// which the datafiles are held in could contain a number.
func ParseID(path string) (uint32, error) {
	re := regexp.MustCompile("[0-9]+")
	matches := re.FindAllString(path, -1)

	if len(matches) == 0 {
		return 0, ErrNoFileID
	}

	// convert it into number
	fileID, err := strconv.ParseUint(matches[len(matches)-1], 10, 32)
	if err != nil {
		return 0, err
	}

	return uint32(fileID), nil
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

func (dfs *DatafileScanner) Scan() (*Entry, error) {
	metaBuffer := make([]byte, 16, 16)
	nBytes, err := dfs.file.ReadAt(metaBuffer, dfs.offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// we are at the end of the file so we should stop reading.
	if err == io.EOF {
		return nil, io.EOF
	}

	// we didn't read enough bytes
	if nBytes != 16 {
		return nil, ErrWrongByteCount
	}
	dfs.offset += int64(nBytes)

	_, timestamp, vsize, ksize := encoder.DecodeEntryMeta(metaBuffer)
	key := make([]byte, ksize)

	nBytes, err = dfs.file.ReadAt(key, dfs.offset)
	if err != nil {
		return nil, err
	}

	if nBytes != int(ksize) {
		return nil, ErrWrongByteCount
	}
	dfs.offset += int64(nBytes)

	value := make([]byte, vsize)
	nBytes, err = dfs.file.ReadAt(value, dfs.offset)
	if err != nil {
		return nil, err
	}

	if nBytes != int(vsize) {
		return nil, ErrWrongByteCount
	}
	dfs.offset += int64(nBytes)

	return &Entry{
		Timestamp: timestamp,
		KeySize:   ksize,
		ValueSize: vsize,
		Key:       key,
		Value:     value,
	}, nil
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
	valOffset := df.offset + 16 + int64(len(key))
	df.offset += int64(sz)

	return &keydir.MemEntry{
		Timestamp: timestamp,
		ValOffset: valOffset,
		ValSize:   uint32(len(value)),
		FileID:    df.id,
	}, nil
}

// Close closes the datafile file pointer and the hint pointer.
func (df *Datafile) Close() {
	df.file.Close()
	df.hintFile.Close()
}

// Offset returns offset to the end of the file.
func (df *Datafile) Offset() int64 {
	return df.offset
}

// ID returns the id the datafile has. This ID is the timestamp of the time when the instance was created.
func (df *Datafile) ID() uint32 {
	return df.id
}

// InitDataFileScanner creates a new scanner that can read entries in a datafile one by one.
func InitDatafileScanner(df *Datafile) *DatafileScanner {
	return &DatafileScanner{
		amount: 0,
		offset: 0,
		file:   df.file,
	}
}
