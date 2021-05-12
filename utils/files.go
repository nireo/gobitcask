package utils

import (
	"fmt"
	"io"
	"os"
)

// CreateDirectoryIfNotExist creates a directory with a given name if it exists
func CreateDirectoryIfNotExist(directory string) error {
	// check if the directory exists
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		// it doesnt exist
		if err := os.Mkdir(directory, 0777); err != nil {
			return err
		}
	}

	return nil
}

// CopyFile takes in a path where to copy from and creates a file at path dst
func CopyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	buf := make([]byte, 1<<16)
	for {
		n, err := source.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		if n == 0 {
			break
		}

		if _, err := destination.Write(buf[:n]); err != nil {
			return 0, err
		}
	}

	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}
