package store

// shardCount is deliberately a power of two so routing a key costs one cheap
// mask after hashing. Thirty-two shards is enough to separate concurrent
// clients on this machine without making cross-shard operations expensive.
const shardCount = 32

// shardIndex uses FNV-1a. It is stable across processes (unlike map hashes),
// inexpensive for the short keys typical of Redis workloads, and sufficiently
// well distributed for lock striping.
func shardIndex(key string) uint8 {
	const (
		offset64 = uint64(14695981039346656037)
		prime64  = uint64(1099511628211)
	)
	h := offset64
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime64
	}
	return uint8(h & (shardCount - 1))
}
