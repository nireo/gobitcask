package datafile_test

import (
	"bytes"
	"log"
	"os"
	"testing"

	"github.com/nireo/bitcask/datafile"
	"github.com/nireo/bitcask/utils"
)

func createTestDirectory(t *testing.T) {
	t.Helper()

	if err := utils.CreateDirectoryIfNotExist("./test"); err != nil {
		t.Fatalf("could not create directory for files.")
	}

	t.Cleanup(func() {
		// remove all the persistance related data
		if err := os.RemoveAll("./test"); err != nil {
			log.Printf("could not delete database folder")
		}
	})
}

func TestWriteRead(t *testing.T) {
	createTestDirectory(t)

	df, err := datafile.NewDatafile("./test")
	if err != nil {
		t.Fatalf("error creating datafile: %s", err)
	}
	defer df.Close()

	entry1, err := df.Write([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("could not write entry1")
	}

	entry2, err := df.Write([]byte("world"), []byte("world"))
	if err != nil {
		t.Fatalf("could not write entry2")
	}

	// read the values from the given offset
	entry1Value, err := df.ReadOffset(entry1.ValOffset, entry1.ValSize)
	if err != nil {
		t.Errorf("could not read value of entry1 from datafile")
	}

	entry2Value, err := df.ReadOffset(entry2.ValOffset, entry2.ValSize)
	if err != nil {
		t.Errorf("could not read value of entry1 from datafile")
	}

	if !bytes.Equal(entry1Value, []byte("world")) {
		t.Errorf("the values don't match")
	}

	if !bytes.Equal(entry2Value, []byte("world")) {
		t.Errorf("the values don't match")
	}
}

func TestParseID(t *testing.T) {
	createTestDirectory(t)

	df, err := datafile.NewDatafile("./test")
	if err != nil {
		t.Fatalf("error creating datafile: %s", err)
	}
	defer df.Close()

	// parse the id from the path
	id, err := datafile.ParseID(df.GetPath("./test"))
	if err != nil {
		t.Errorf("error parsing id from path")
	}

	if id != df.ID() {
		t.Errorf("the ids didn't match. got=%d want=%d", id, df.ID())
	}
}

func TestDatafileScanner(t *testing.T) {
	createTestDirectory(t)

	df, err := datafile.NewDatafile("./test")
	if err != nil {
		t.Fatalf("error creating datafile: %s", err)
	}
	defer df.Close()

	_, err = df.Write([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("could not write entry1")
	}

	_, err = df.Write([]byte("world"), []byte("world"))
	if err != nil {
		t.Fatalf("could not write entry2")
	}

	scanner := datafile.InitDatafileScanner(df)

	fentry1, err := scanner.Scan()
	if err != nil {
		t.Errorf("error scanning first entry: %s", err)
	}

	fentry2, err := scanner.Scan()
	if err != nil {
		t.Errorf("error scanning second entry: %s", err)
	}

	if !bytes.Equal(fentry1.Key, []byte("hello")) {
		t.Errorf("the keys don't match")
	}

	if !bytes.Equal(fentry2.Key, []byte("world")) {
		t.Errorf("the keys don't match")
	}

	if !bytes.Equal(fentry1.Value, []byte("world")) {
		t.Errorf("the keys don't match")
	}

	if !bytes.Equal(fentry2.Value, []byte("world")) {
		t.Errorf("the keys don't match")
	}
}
