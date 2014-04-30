package hash

import (
	"hash/fnv"
)

// Local retrieves the hash value for an object id for the local shard.
func Local(id string) uint32 {
	return uint32(hash(id) & 0xFFFFFFFF)
}

// Remote retrieves the hash value for an object id for the remote shard.
func Remote(id string) uint32 {
	return uint32(hash(id) >> 32)
}

func hash(id string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(id))
	return h.Sum64()
}
