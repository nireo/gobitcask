package bitcask_test

import (
	"log"
	"os"
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
