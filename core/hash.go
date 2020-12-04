package core

import (
	"encoding/binary"
	"sort"

	"github.com/spaolacci/murmur3"
)

type sortNodeInfo []NodeInfo

var _ sort.Interface = sortNodeInfo{}

func (s sortNodeInfo) Len() int {
	return len(s)
}

func (s sortNodeInfo) Less(i, j int) bool {
	if s[i].Hash < s[j].Hash {
		return true
	} else if s[i].Hash > s[j].Hash {
		return false
	} else {
		return s[i].NodeID > s[j].NodeID
	}
}

func (s sortNodeInfo) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

// HashUint32 creates a hash using murmur3
func HashUint32(num uint32) Hash {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], num)
	return Hash(murmur3.Sum32(buf[:]))
}

// Sort sorts the node infos
func Sort(nodes []NodeInfo) {
	sort.Sort(sortNodeInfo(nodes))
}

// Equals compare for equality
func Equals(a, b []NodeInfo) bool {
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

type nullNodeInfo struct {
	Valid bool
	Node  NodeInfo
}

func getConsistentHashingElem(sortedNodes []NodeInfo, hash Hash) nullNodeInfo {
	if len(sortedNodes) == 0 {
		return nullNodeInfo{
			Valid: false,
		}
	}

	n := len(sortedNodes)
	first := 0
	last := n

	for first != last {
		mid := (first + last) / 2
		midValue := sortedNodes[mid].Hash

		if midValue < hash {
			first = mid + 1
		} else {
			last = mid
		}
	}

	if first == n {
		return nullNodeInfo{
			Valid: true,
			Node:  sortedNodes[0],
		}
	}

	return nullNodeInfo{
		Valid: true,
		Node:  sortedNodes[first],
	}
}

// GetNodeAddress returns the address of node for consistent hashing
func GetNodeAddress(sortedNodes []NodeInfo, hash Hash) NullAddress {
	nullNode := getConsistentHashingElem(sortedNodes, hash)
	if !nullNode.Valid {
		return NullAddress{
			Valid: false,
		}
	}
	return NullAddress{
		Valid:   true,
		Address: nullNode.Node.Address,
	}
}

// GetNodeID returns the nodeID of node for consistent hashing
func GetNodeID(sortedNodes []NodeInfo, hash Hash) NullNodeID {
	nullNode := getConsistentHashingElem(sortedNodes, hash)
	if !nullNode.Valid {
		return NullNodeID{
			Valid: false,
		}
	}
	return NullNodeID{
		Valid:  true,
		NodeID: nullNode.Node.NodeID,
	}
}
