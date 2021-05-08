package hint_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nireo/bitcask/hint"
)

func TestWritingToFile(t *testing.T) {
	timestamp := uint32(time.Now().Unix())
	directory := "./test"

	// check if the directory exists
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		// it doesnt exist
		if err := os.Mkdir(directory, 0777); err != nil {
			t.Errorf("could not create directory: %s", err)
		}
	}
	// create a new hint file

	hintFile, err := hint.NewHintFile(directory, timestamp)
	if err != nil {
		t.Errorf("could not create hint file: %s", err)
	}

	if err := hintFile.Append(timestamp, 20, 0, []byte("testkey")); err != nil {
		t.Errorf("could not append to hint file %s", err)
	}

	// test that writing happens with

	if err := os.RemoveAll(directory); err != nil {
		t.Errorf("could not delete the directory")
	}
}

func TestEncodingDecoding(t *testing.T) {
	timestamp := uint32(time.Now().Unix())
	directory := "./test"

	// check if the directory exists
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		// it doesnt exist
		if err := os.Mkdir(directory, 0777); err != nil {
			t.Errorf("could not create directory: %s", err)
		}
	}
	// create a new hint file
	hintFile, err := hint.NewHintFile(directory, timestamp)
	if err != nil {
		t.Errorf("could not create hint file: %s", err)
	}

	vsize := uint32(200)
	offset := int64(200)
	key := []byte("helloworld")

	if err := hintFile.Append(timestamp, vsize, offset, key); err != nil {
		t.Errorf("could not append to hint file %s", err)
	}
	hintFile.Close()

	// decode the data
	data, err := ioutil.ReadFile(filepath.Join(directory, fmt.Sprintf("%v.hnt", timestamp)))
	if err != nil {
		t.Errorf("error reading data from the file: %s", err)
	}

	timestamp2, vsize2, offset2, key2, nBytes := hint.DecodeHint(data)
	if int(nBytes) != len(data) {
		t.Errorf("wrong amount of data read")
	}

	if timestamp != timestamp2 {
		t.Errorf("non matching timestamps: want=%v got=%v", timestamp, timestamp2)
	}

	if offset != offset2 {
		t.Errorf("non matching offsets: want=%d got=%d", offset, offset2)
	}

	if vsize != vsize2 {
		t.Errorf("non matching value sizes: want=%d got=%d", vsize, vsize2)
	}

	if !bytes.Equal(key, key2) {
		t.Errorf("non matching keys: want=%s got=%s", string(key), string(key2))
	}

	if err := os.RemoveAll(directory); err != nil {
		t.Errorf("could not delete the directory")
	}
}
