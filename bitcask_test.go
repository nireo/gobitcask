package bitcask_test

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/nireo/bitcask"
)

func createTestDatabase(t *testing.T) *bitcask.DB {
	t.Helper()
	db, err := bitcask.Open("./data", nil)
	if err != nil {
		t.Fatalf("could not create a database instance: %s", err)
	}

	t.Cleanup(func() {
		// remove all the persistance related data
		if err := os.RemoveAll(db.GetDirectory()); err != nil {
			log.Printf("could not delete database folder")
		}
	})

	return db
}

func TestDirectoryCreated(t *testing.T) {
	db := createTestDatabase(t)

	if _, err := os.Stat(db.GetDirectory()); os.IsNotExist(err) {
		t.Errorf("could not create new directory")
	}
}

func TestWritableFileCreated(t *testing.T) {
	db := createTestDatabase(t)

	files, err := ioutil.ReadDir(db.GetDirectory())
	if err != nil {
		t.Errorf("error reading files from directory: %s", err)
	}

	count := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".df") {
			count++
		}
	}

	if count != 1 {
		t.Errorf("a writable file was not found")
	}
}

func TestBasicOperations(t *testing.T) {
	db := createTestDatabase(t)

	stored := []string{}
	for i := 0; i < 1000; i++ {
		randNumber := strconv.Itoa(rand.Int())

		if err := db.Put([]byte(randNumber), []byte("value"+randNumber)); err != nil {
			t.Errorf("error putting value into database.")
		}

		stored = append(stored, randNumber)
	}

	for _, key := range stored {
		if _, err := db.Get([]byte(key)); err != nil {
			t.Errorf("could not get key %s", key)
		}
	}
}
