package storage

import (
	"github.com/jrife/ptarmigan/flock/server/flockpb"
)

type Transaction interface {
	Guard() []Predicate
	Success()
	Failure()
}

type Predicate interface {
	Selection()
	Condition()
}

// type MVCCTxn struct {
// 	Guard []flockpb.Compare
// 	Success []flockpb.KVRequestOp
// 	Failure []flockpb.KVRequestOp
// }

type MVCCStore interface {
	Txn(flockpb.KVTxnRequest) (flockpb.KVTxnResponse, error)
}

func ExecuteTransaction(txn flockpb.KVTxnRequest) (flockpb.KVTxnResponse, error) {
	var ops []*flockpb.KVRequestOp

	// for _, guard := range txn.Compare {
	// 	selection := guard.Selection
	// 	predicate := guard.Predicate

	// 	for each kv in selection {
	// 		if predicate is false with kv {
	// 			stop looping through guards and set success to false
	// 		}
	// 	}
	// }

	// Execute ops
	for _, op := range ops {
		switch op.GetRequest().(type) {
		case *flockpb.KVRequestOp_RequestQuery:
		case *flockpb.KVRequestOp_RequestPut:
		case *flockpb.KVRequestOp_RequestDelete:
		case *flockpb.KVRequestOp_RequestTxn:
		}
	}

	return flockpb.KVTxnResponse{}, nil
}

type MVCCTxn interface {
	EvaluateGuard([]flockpb.Compare) (bool, error)
}
