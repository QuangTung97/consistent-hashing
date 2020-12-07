package core

import (
	"context"
)

type (
	// NodeID for node id
	NodeID uint32

	// Hash for hash value
	Hash uint32

	// NodeInfo keeps info for a node
	NodeInfo struct {
		NodeID  NodeID
		Hash    Hash
		Address string
	}

	// NullAddress represent valid or invalid node address
	NullAddress struct {
		Valid   bool
		Address string
	}

	// NullNodeID for nullable node id
	NullNodeID struct {
		Valid  bool
		NodeID NodeID
	}

	//WatchResponse for each watch response
	WatchResponse struct {
		Nodes []NodeInfo
	}

	// Service for storing consistent hashing
	Service interface {
		// KeepAliveAndWatch must delete the info when context is Done
		// and watch see the changes of node infos
		// nodes in WatchResponse always sorted by hash and are immutable
		KeepAliveAndWatch(ctx context.Context, info NodeInfo, ch chan<- WatchResponse)
		// Watch ...
		Watch(ctx context.Context, ch chan<- WatchResponse)
	}
)

// DifferenceResult result of ComputeAddressesDifference
type DifferenceResult struct {
	Deleted  []string
	Inserted []string
}

// ComputeAddressesDifference computes the difference
func ComputeAddressesDifference(oldNodes []NodeInfo, newNodes []NodeInfo,
) DifferenceResult {
	oldAddressSet := make(map[string]struct{})
	newAddressSet := make(map[string]struct{})

	for _, n := range oldNodes {
		oldAddressSet[n.Address] = struct{}{}
	}

	for _, n := range newNodes {
		newAddressSet[n.Address] = struct{}{}
	}

	var deleted []string
	for _, n := range oldNodes {
		_, existed := newAddressSet[n.Address]
		if !existed {
			deleted = append(deleted, n.Address)
		}
	}

	var inserted []string
	for _, n := range newNodes {
		_, existed := oldAddressSet[n.Address]
		if !existed {
			inserted = append(inserted, n.Address)
		}
	}

	return DifferenceResult{
		Deleted:  deleted,
		Inserted: inserted,
	}
}
