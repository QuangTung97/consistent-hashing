package core

import (
	"encoding/binary"
	"sort"

	"github.com/spaolacci/murmur3"
)

type sortConsistentHash []ConsistentHash

var _ sort.Interface = sortConsistentHash{}

func (s sortConsistentHash) Len() int {
	return len(s)
}

func (s sortConsistentHash) Less(i, j int) bool {
	if s[i].Hash < s[j].Hash {
		return true
	} else if s[i].Hash > s[j].Hash {
		return false
	} else {
		return s[i].NodeID > s[j].NodeID
	}
}

func (s sortConsistentHash) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

// HashUint32 creates a hash using murmur3
func HashUint32(num uint32) Hash {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], num)
	return Hash(murmur3.Sum32(buf[:]))
}

// Sort sorts the hashes
func Sort(hashes []ConsistentHash) {
	sort.Sort(sortConsistentHash(hashes))
}

// Equals compare for equality
func Equals(a, b []ConsistentHash) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

type nullConsistentHash struct {
	Valid bool
	Hash  ConsistentHash
}

func getConsistentHashingElem(sortedHashes []ConsistentHash, hash Hash) nullConsistentHash {
	if len(sortedHashes) == 0 {
		return nullConsistentHash{
			Valid: false,
		}
	}

	n := len(sortedHashes)
	first := 0
	last := n

	for first != last {
		mid := (first + last) / 2
		midValue := sortedHashes[mid].Hash

		if midValue < hash {
			first = mid + 1
		} else {
			last = mid
		}
	}

	if first == n {
		return nullConsistentHash{
			Valid: true,
			Hash:  sortedHashes[0],
		}
	}

	return nullConsistentHash{
		Valid: true,
		Hash:  sortedHashes[first],
	}
}

// GetNodeAddress returns the address of node for consistent hashing
func GetNodeAddress(sortedHashes []ConsistentHash, hash Hash) NullAddress {
	nullHash := getConsistentHashingElem(sortedHashes, hash)
	if !nullHash.Valid {
		return NullAddress{
			Valid: false,
		}
	}
	return NullAddress{
		Valid:   true,
		Address: nullHash.Hash.Address,
	}
}

// GetNodeID
func GetNodeID(sortedHashes []ConsistentHash, hash Hash) NullNodeID {
	nullHash := getConsistentHashingElem(sortedHashes, hash)
	if !nullHash.Valid {
		return NullNodeID{
			Valid: false,
		}
	}
	return NullNodeID{
		Valid:  true,
		NodeID: nullHash.Hash.NodeID,
	}
}
