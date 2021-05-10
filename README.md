# bitcask

This is a golang implementation of the [bitcask](https://riak.com/assets/bitcask-intro.pdf) key-value database. It prioritized high performance and fault-tolerance.

## Usage

This displays comman usage of the database. The second parameters passed into database are the options.

```go
package main

import (
	"bytes"
	"log"

	"github.com/nireo/bitcask"
)

func main() {
	db, err := bitcask.Open("./data", nil)
	if err != nil {
		log.Fatalf("could not open the database: %s", err)
	}
	defer db.Close()

	key := []byte("hello")
	value := []byte("world")

	if err := db.Put(key, value); err != nil {
		log.Fatalf("could put into database: %s", err)
	}

	val, err := db.Get(key)
	if err != nil {
		log.Fatalf("could key from the database: %s", err)
	}

	if !bytes.Equal(val, value) {
		log.Fatalf("the values don't match")
	}
}
```

## Pros & Cons

I think that this isn't for every single use case, but it really depends on the situtiation.

### Pros
* Simple Code
* Very high throughput
* Predictable performance

### Cons
* High memory usage
* Can take more disk space since metadata is also written to disk
