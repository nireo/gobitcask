package utils_test

import (
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/nireo/bitcask/utils"
)

func TestCopyFile(t *testing.T) {
	file, err := os.Create("./test1")
	if err != nil {
		t.Fatalf("could not create test1 file: %s", err)
	}

	for i := 0; i < 1e3; i++ {
		dataToWrite := strconv.Itoa(rand.Int())
		file.Write([]byte(dataToWrite))
	}
	file.Close()

	if _, err := utils.CopyFile("./test1", "./test2"); err != nil {
		t.Fatalf("could not copy files: %s", err)
	}

	// read data from both files and compare that data
	data1, err := ioutil.ReadFile("./test1")
	if err != nil {
		t.Fatalf("could not read data from 'test1': %s", err)
	}

	data2, err := ioutil.ReadFile("./test2")
	if err != nil {
		t.Fatalf("could not read data from 'test2': %s", err)
	}

	if !reflect.DeepEqual(data1, data2) {
		t.Errorf("the files are not equal")
	}

	if err := os.Remove("./test1"); err != nil {
		t.Errorf("could not remove 'test1' file: %s", err)
	}

	if err := os.Remove("./test2"); err != nil {
		t.Errorf("could not remove 'test2' file: %s", err)
	}
}
