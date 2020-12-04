package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNodeAddress(t *testing.T) {
	table := []struct {
		name     string
		nodes    []NodeInfo
		hash     Hash
		expected NullAddress
	}{
		{
			name: "empty",
		},
		{
			name: "single",
			nodes: []NodeInfo{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
			},
			hash: 200,
			expected: NullAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "single-wrap-around",
			nodes: []NodeInfo{
				{
					NodeID:  1,
					Hash:    200,
					Address: "node1",
				},
			},
			hash: 100,
			expected: NullAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "two-middle",
			nodes: []NodeInfo{
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
			expected: NullAddress{
				Valid:   true,
				Address: "node2",
			},
		},
		{
			name: "two-middle",
			nodes: []NodeInfo{
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
			expected: NullAddress{
				Valid:   true,
				Address: "node2",
			},
		},
		{
			name: "two-after",
			nodes: []NodeInfo{
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
			expected: NullAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "two-after",
			nodes: []NodeInfo{
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
			expected: NullAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "two-begin",
			nodes: []NodeInfo{
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
			expected: NullAddress{
				Valid:   true,
				Address: "node1",
			},
		},
		{
			name: "two-begin",
			nodes: []NodeInfo{
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
			expected: NullAddress{
				Valid:   true,
				Address: "node1",
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			result := GetNodeAddress(e.nodes, e.hash)
			assert.Equal(t, e.expected, result)
		})
	}
}

func TestSort(t *testing.T) {
	table := []struct {
		name     string
		nodes    []NodeInfo
		expected []NodeInfo
	}{
		{
			name: "empty",
		},
		{
			name: "one",
			nodes: []NodeInfo{
				{
					NodeID: 1,
					Hash:   100,
				},
			},
			expected: []NodeInfo{
				{
					NodeID: 1,
					Hash:   100,
				},
			},
		},
		{
			name: "two-difference-hash",
			nodes: []NodeInfo{
				{
					NodeID: 2,
					Hash:   200,
				},
				{
					NodeID: 1,
					Hash:   100,
				},
			},
			expected: []NodeInfo{
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
			nodes: []NodeInfo{
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
			expected: []NodeInfo{
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
			Sort(e.nodes)
			assert.Equal(t, e.expected, e.nodes)
		})
	}
}
