package hint

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nireo/bitcask/keydir"
)

// HintFile represents a hint file that has
type HintFile struct {
	File *os.File
}

type hintScanner struct {
	*bufio.Scanner
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

func initScanner(file *os.File) *hintScanner {
	s := bufio.NewScanner(file)
	buffer := make([]byte, 4096)
	s.Buffer(buffer, bufio.MaxScanTokenSize)
	s.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		_, _, _, _, bytesRead := DecodeHint(data)
		if bytesRead == 0 {
			return int(bytesRead), data[:bytesRead], nil
		}

		return 0, nil, nil
	})

	return &hintScanner{s}
}

func (hs *hintScanner) next() (*keydir.MemEntry, []byte, error) {
	hs.Scan()
	timestamp, vsize, offset, key, bytesRead := DecodeHint(hs.Bytes())
	if bytesRead == 0 {
		return nil, nil, errors.New("could not read entry")
	}

	return &keydir.MemEntry{
		Timestamp: timestamp,
		ValSize:   vsize,
		ValOffset: offset,
	}, key, nil
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
	key := buffer[20:ksize]

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

func AppendPathToKeyDir(path string, dataFileID uint32, kd *keydir.KeyDir) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := initScanner(f)
	for {
		mementry, key, err := scanner.next()
		if err != nil {
			break
		}

		mementry.FileID = dataFileID
		kd.Put(string(key), mementry)
	}

	return nil
}
