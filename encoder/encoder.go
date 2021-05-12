package encoder

import (
	"encoding/binary"
	"errors"
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
	binary.LittleEndian.PutUint32(buffer[:4], crc)

	return buffer
}

// DecodeEntryValue takes in some data and decodes the value from the data.
func DecodeEntryValue(data []byte) ([]byte, error) {
	ksize := binary.LittleEndian.Uint32(data[8:12])
	vsize := binary.LittleEndian.Uint32(data[12:20])

	value := make([]byte, vsize)

	// copy the value from the buffer
	copy(value, data[(16+ksize):(16+ksize+vsize)])

	c32 := binary.LittleEndian.Uint32(data[:4])
	if crc32.ChecksumIEEE(data[4:]) != c32 {
		return nil, errors.New("the crc32 checksum doesn't match")
	}

	return value, nil
}

// DecodeAll returns all of the information and returns all of the variables.
func DecodeAll(data []byte) (timestamp, ksize, vsize uint32, key, value []byte, err error) {
	if len(data) < 17 {
		return 0, 0, 0, nil, nil, errors.New("too few bytes to properly read")
	}

	timestamp = binary.LittleEndian.Uint32(data[4:8])
	ksize = binary.LittleEndian.Uint32(data[8:12])
	vsize = binary.LittleEndian.Uint32(data[12:16])

	key = make([]byte, ksize)
	value = make([]byte, vsize)
	copy(key, data[16:16+ksize])
	copy(value, data[16+ksize:16+ksize+vsize])

	crc := binary.LittleEndian.Uint32(data[0:4])
	if crc32.ChecksumIEEE(data[4:]) != crc {
		return 0, 0, 0, nil, nil, errors.New("the crc32 checksum doesn't match")
	}
	err = nil

	return
}
