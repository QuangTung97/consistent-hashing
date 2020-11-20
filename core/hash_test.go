package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNodeAddress(t *testing.T) {
	table := []struct {
		name     string
		hashes   []ConsistentHash
		hash     Hash
		expected NodeAddress
	}{
		{
			name: "empty",
		},
		{
			name: "single",
			hashes: []ConsistentHash{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
			},
			hash: 200,
			expected: NodeAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "single-wrap-around",
			hashes: []ConsistentHash{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
			},
			hash: 100,
			expected: NodeAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "two-middle",
			hashes: []ConsistentHash{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
				{
					NodeID:  2,
					Hash:    400,
					Address: "node2",
				},
			},
			hash: 201,
			expected: NodeAddress{
				Valid:   true,
				Address: "node2",
			},
		},
		{
			name: "two-middle",
			hashes: []ConsistentHash{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
				{
					NodeID:  2,
					Hash:    400,
					Address: "node2",
				},
			},
			hash: 400,
			expected: NodeAddress{
				Valid:   true,
				Address: "node2",
			},
		},
		{
			name: "two-after",
			hashes: []ConsistentHash{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
				{
					NodeID:  2,
					Hash:    400,
					Address: "node2",
				},
			},
			hash: 401,
			expected: NodeAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "two-after",
			hashes: []ConsistentHash{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
				{
					NodeID:  2,
					Hash:    400,
					Address: "node2",
				},
			},
			hash: 5000,
			expected: NodeAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "two-begin",
			hashes: []ConsistentHash{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
				{
					NodeID:  2,
					Hash:    400,
					Address: "node2",
				},
			},
			hash: 0,
			expected: NodeAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "two-begin",
			hashes: []ConsistentHash{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
				{
					NodeID:  2,
					Hash:    400,
					Address: "node2",
				},
			},
			hash: 200,
			expected: NodeAddress{
				Valid:   true,
				Address: "node1",
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			result := GetNodeAddress(e.hashes, e.hash)
			assert.Equal(t, e.expected, result)
		})
	}
}

func TestSort(t *testing.T) {
	table := []struct {
		name     string
		hashes   []ConsistentHash
		expected []ConsistentHash
	}{
		{
			name: "empty",
		},
		{
			name: "one",
			hashes: []ConsistentHash{
				{
					NodeID: 1,
					Hash:   100,
				},
			},
			expected: []ConsistentHash{
				{
					NodeID: 1,
					Hash:   100,
				},
			},
		},
		{
			name: "two-difference-hash",
			hashes: []ConsistentHash{
				{
					NodeID: 2,
					Hash:   200,
				},
				{
					NodeID: 1,
					Hash:   100,
				},
			},
			expected: []ConsistentHash{
				{
					NodeID: 1,
					Hash:   100,
				},
				{
					NodeID: 2,
					Hash:   200,
				},
			},
		},
		{
			name: "three-same-hash",
			hashes: []ConsistentHash{
				{
					NodeID: 2,
					Hash:   200,
				},
				{
					NodeID: 3,
					Hash:   200,
				},
				{
					NodeID: 1,
					Hash:   100,
				},
			},
			expected: []ConsistentHash{
				{
					NodeID: 1,
					Hash:   100,
				},
				{
					NodeID: 3,
					Hash:   200,
				},
				{
					NodeID: 2,
					Hash:   200,
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			Sort(e.hashes)
			assert.Equal(t, e.expected, e.hashes)
		})
	}
}
