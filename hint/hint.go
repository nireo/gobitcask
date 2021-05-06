package hint

import (
	"encoding/binary"
	"errors"
	"os"
)

// HintFile represents a hint file that has
type HintFile struct {
	File *os.File
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
