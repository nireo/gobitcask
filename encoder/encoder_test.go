package encoder_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/nireo/bitcask/encoder"
)

func TestEncodeDecode(t *testing.T) {
	ts := uint32(time.Now().Unix())
	data := encoder.EncodeEntry([]byte("hello"), []byte("world"), ts)

	// we don't need the key and value size since we check if the bytes are equal.
	ts, _, _, key, value, err := encoder.DecodeAll(data)
	if err != nil {
		t.Errorf("could not decode entry: %s", err)
	}

	if !bytes.Equal(key, []byte("hello")) {
		t.Errorf("the keys dont match. got=%s want=%s", string(key), []byte("hello"))
	}

	if !bytes.Equal(value, []byte("world")) {
		t.Errorf("the values don't match. got=%s want=%s", string(value), []byte("world"))
	}
}
