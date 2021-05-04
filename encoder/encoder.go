package encoder

import (
	"encoding/binary"
	"hash/crc32"
)

// EncodeEntry takes in a key, value and timestamp and then creates a buffer containing
// all of the data from that. This data is appended to a datafile.
func EncodeEntry(key []byte, value []byte, ts uint32) []byte {
	// the header contains the first 16 bytes denoting the crc, timestamp, keysize
	// value size and then followed by the lengths of key, and value.

	buffer := make([]byte, 16)
	binary.LittleEndian.PutUint32(buffer[4:8], ts)
	binary.LittleEndian.PutUint32(buffer[8:12], uint32(len(key)))
	binary.LittleEndian.PutUint32(buffer[12:16], uint32(len(value)))

	buffer = append(buffer[:], key[:]...)
	buffer = append(buffer[:], value[:]...)

	crc := crc32.ChecksumIEEE(buffer[4:])
	binary.LittleEndian.PutUint32(buffer[4:8], crc)

	return buffer
}
