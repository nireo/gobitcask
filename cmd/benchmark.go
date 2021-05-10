package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/nireo/bitcask"
)

var amount = flag.Int("amount", 10000, "the amount of items to write and get from the database")
var testRead = flag.Bool("read", false, "if the program should also benchmark reading the values")

func init() {
	flag.Parse()
}

func main() {
	rand.Seed(time.Now().UnixNano())
	db, err := bitcask.Open("./benchmark", nil)
	if err != nil {
		log.Fatalf("could not start database")
	}
	defer db.Close()

	startTime := time.Now()
	keys := []string{}
	for i := 0; i < *amount; i++ {
		randString := strconv.Itoa(rand.Int())
		if err := db.Put([]byte(randString), []byte("val-"+randString)); err != nil {
			fmt.Printf("could not write value %s\n", randString)
		}
		keys = append(keys, randString)
	}

	log.Printf("writes took %v", time.Since(startTime))

	if *testRead != false {
		readStart := time.Now()
		for _, key := range keys {
			_, err := db.Get([]byte(key))
			if err != nil {
				log.Printf("error getting key: %s", err)
			}
		}
		log.Printf("reads took %v", time.Since(readStart))
	}

	if err := os.RemoveAll(db.GetDirectory()); err != nil {
		log.Printf("could not delete directory: %s", err)
	}
}
