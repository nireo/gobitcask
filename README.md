# bitcask

This is a golang implementation of the [bitcask](https://riak.com/assets/bitcask-intro.pdf) key-value database. It prioritized high performance and fault-tolerance.

## Pros & Cons

I think that this isn't for every single use case, but it really depends on the situtiation.

### Pros
* Simple Code
* Very high throughput
* Predictable performance

### Cons
* High memory usage
* Can take more disk space since metadata is also written to disk
