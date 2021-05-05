package bitcask

import (
	"github.com/nireo/bitcask/datafile"
	"github.com/nireo/bitcask/keydir"
)

type Options struct {
}

type DB struct {
	Options *Options
	KeyDir  *keydir.KeyDir
	Manager *datafile.DatafileManager
}
