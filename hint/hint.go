package hint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nireo/bitcask/encoder"
	"github.com/nireo/bitcask/keydir"
)

var (
	ErrWrongByteCount = errors.New("wrote wrong amount of bytes to file.")
)

// HintFile represents a hint file that has
type HintFile struct {
	File *os.File
}

type HintScanner struct {
	offset int64
	file   *os.File
}

// Close closes the file pointer
func (hf *HintFile) Close() {
	hf.File.Close()
}

// Append compiles data for a hint entry and appends to the end of the file pointer
func (hf *HintFile) Append(timestamp, vsize uint32, offset int64, key []byte) error {
	buffer := encoder.EncodeHint(timestamp, vsize, offset, key)
	nBytes, err := hf.File.Write(buffer)
	if err != nil {
		return err
	}

	if nBytes != len(buffer) {
		return errors.New("wrong amount of bytes written: write failed.")
	}

	return nil
}

func (hfs *HintScanner) Scan() (*keydir.MemEntry, []byte, error) {
	metaBuffer := make([]byte, 20, 20)
	nBytes, err := hfs.file.ReadAt(metaBuffer, hfs.offset)
	if err != nil {
		return nil, nil, err
	}

	// we didn't read enough bytes
	if nBytes != 20 {
		return nil, nil, ErrWrongByteCount
	}
	hfs.offset += int64(nBytes)

	timestamp, ksize, vsize, offset := encoder.DecodeHintMeta(metaBuffer)
	key := make([]byte, ksize)

	nBytes, err = hfs.file.ReadAt(key, hfs.offset)
	if err != nil {
		return nil, nil, err
	}

	if nBytes != int(ksize) {
		return nil, nil, ErrWrongByteCount
	}
	hfs.offset += int64(nBytes)

	return &keydir.MemEntry{
		Timestamp: timestamp,
		ValOffset: offset,
		ValSize:   vsize,
	}, key, nil
}

// InitDataFileScanner creates a new scanner that can read entries in a datafile one by one.
func InitHintScanner(hintFile *os.File) *HintScanner {
	return &HintScanner{
		offset: 0,
		file:   hintFile,
	}
}

// NewHintFile creates a new hint file from a timestamp
func NewHintFile(directory string, timestamp uint32) (*HintFile, error) {
	path := filepath.Join(directory, fmt.Sprintf("%v.hnt", timestamp))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &HintFile{
		File: f,
	}, nil
}

// AppendPathToKeyDir takes a hint file from path and then fills the given keydirectory pointer with
// the key meta-data in the files. The dataFileID is also needed since it isn't stored in the hint-file.
func AppendPathToKeyDir(path string, dataFileID uint32, kd *keydir.KeyDir) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := InitHintScanner(f)
	for {
		mementry, key, err := scanner.Scan()
		if err != nil {
			break
		}

		mementry.FileID = dataFileID
		kd.Put(string(key), mementry)
	}

	return nil
}
