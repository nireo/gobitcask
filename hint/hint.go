package hint

import (
	"encoding/binary"
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
	// This buffer contains the timestamp, key and value size and finally the entire key.
	buffer := make([]byte, 20)
	binary.LittleEndian.PutUint32(buffer[0:4], timestamp)
	binary.LittleEndian.PutUint32(buffer[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint32(buffer[8:12], vsize)
	binary.LittleEndian.PutUint64(buffer[12:20], uint64(offset))

	buffer = append(buffer[:], key[:]...)
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

// DecodeHint returns all of information stored in a mementry and lastly it also returns
// the amount of bytes read. Such that the scanning through the values works better.
func DecodeHint(buffer []byte) (uint32, uint32, int64, []byte, uint32) {
	if len(buffer) < 20 {
		return 0, 0, 0, nil, 0
	}

	timestamp := binary.LittleEndian.Uint32(buffer[:4])
	vsize := binary.LittleEndian.Uint32(buffer[8:12])
	offset := binary.LittleEndian.Uint64(buffer[12:20])

	ksize := binary.LittleEndian.Uint32(buffer[4:8])
	key := buffer[20 : ksize+20]

	return timestamp, vsize, int64(offset), key, 20 + ksize
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
