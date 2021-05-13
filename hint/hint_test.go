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
	"github.com/nireo/bitcask/keydir"
)

func createDirectoryIfNotExists(t *testing.T, directory string) {
	t.Helper()

	if _, err := os.Stat(directory); os.IsNotExist(err) {
		if err := os.Mkdir(directory, 0777); err != nil {
			t.Errorf("could not create directory: %s", err)
		}
	}

	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("could not delete the directory")
		}
	})
}

func TestWritingToFile(t *testing.T) {
	timestamp := uint32(time.Now().Unix())
	directory := "./test"

	createDirectoryIfNotExists(t, directory)

	// create a new hint file
	hintFile, err := hint.NewHintFile(directory, timestamp)
	if err != nil {
		t.Errorf("could not create hint file: %s", err)
	}

	if err := hintFile.Append(timestamp, 20, 0, []byte("testkey")); err != nil {
		t.Errorf("could not append to hint file %s", err)
	}
}

func TestEncodingDecoding(t *testing.T) {
	timestamp := uint32(time.Now().Unix())
	directory := "./test"

	createDirectoryIfNotExists(t, directory)

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
}

func TestFillKeyDir(t *testing.T) {
	timestamp := uint32(time.Now().Unix())
	directory := "./test"

	createDirectoryIfNotExists(t, directory)
	// create a new hint file
	hintFile, err := hint.NewHintFile(directory, timestamp)
	if err != nil {
		t.Errorf("could not create hint file: %s", err)
	}

	vsize := uint32(200)
	offset := int64(200)
	keys := []string{"test1", "test2", "test3", "test4"}

	for _, key := range keys {
		if err := hintFile.Append(timestamp, vsize, offset, []byte(key)); err != nil {
			t.Errorf("could not append to hint file %s", err)
		}
	}
	hintFile.Close()

	kd := keydir.NewKeyDir()
	if err := hint.AppendPathToKeyDir(
		filepath.Join(directory, fmt.Sprintf("%v.hnt", timestamp)),
		timestamp,
		kd,
	); err != nil {
		t.Errorf("error reading key directory from the hint file: %s", err)
	}

	for _, key := range keys {
		entry := kd.Get(key)
		if entry == nil {
			t.Errorf("a key was not found in the key directory")
		}
	}
}

func TestScanner(t *testing.T) {
	timestamp := uint32(time.Now().Unix())
	directory := "./test"

	createDirectoryIfNotExists(t, directory)

	hintfile, err := hint.NewHintFile(directory, timestamp)
	if err != nil {
		t.Errorf("could not create hint file: %s", err)
	}

	keys := []string{"test1", "test2", "test3", "test4", "test5"}
	vsize := uint32(200)
	offset := int64(200)
	for _, key := range keys {
		if err := hintfile.Append(timestamp, vsize, offset, []byte(key)); err != nil {
			t.Errorf("could not append to hint file %s", err)
		}
	}

	hintfile.Close()

	f, err := os.Open(filepath.Join(directory, fmt.Sprintf("%v.hnt", timestamp)))
	if err != nil {
		t.Errorf("error opening hint file")
	}
	defer f.Close()

	scanner := hint.InitHintScanner(f)
	for _, key := range keys {
		mementry, key2, err := scanner.Scan()
		if err != nil {
			t.Errorf("could not read key from file")
			break
		}

		if string(key2) != key {
			t.Errorf("keys don't match: %s", err)
		}

		if mementry.Timestamp != timestamp {
			t.Errorf("timestamps don't match: want=%d got=%d", timestamp, mementry.Timestamp)
		}

		if mementry.ValOffset != offset {
			t.Errorf("offsets don't match: want=%d got=%d", offset, mementry.ValOffset)
		}

		if mementry.ValSize != vsize {
			t.Errorf("value sizes don't match: want=%d got=%d", vsize, mementry.ValSize)
		}
	}
}
