package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jrife/flock/ptarmigan/server/ptarmiganpb"
	"github.com/jrife/flock/ptarmigan/storage"
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/plugins"
	"github.com/jrife/flock/storage/mvcc"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
	"github.com/leanovate/gopter/gen"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type tempStoreBuilder func(t testing.TB) (storage.Store, func())

func builder(plugin kv.Plugin) tempStoreBuilder {
	return func(t testing.TB) (storage.Store, func()) {
		kvRootStore, err := plugins.Plugin("bbolt").NewTempRootStore()

		if err != nil {
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		kvStore := kvRootStore.Store([]byte("test"))

		if err := kvStore.Create(); err != nil {
			kvRootStore.Delete()
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		mvccStore, err := mvcc.New(kvStore)

		if err != nil {
			kvStore.Delete()
			kvRootStore.Delete()
			t.Fatalf("expected err to be nil, got %#v", err)
		}

		cleanup := func() {
			mvccStore.Close()
			kvStore.Delete()
			kvRootStore.Delete()
		}

		atom := zap.NewAtomicLevel()
		logger := zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zapcore.Lock(os.Stdout),
			atom,
		))
		atom.SetLevel(zap.DebugLevel)

		return storage.New(storage.StoreConfig{
			Store:  mvccStore,
			Logger: logger,
		}), cleanup
	}
}

type transaction struct {
	Txn         *ptarmiganpb.KVTxnRequest
	Compact     *int64
	CreateLease *int64
	RevokeLease *int64
}

type transactionResponse struct {
	Txn            *ptarmiganpb.KVTxnResponse
	TxnErr         error
	CompactErr     error
	CreateLease    *ptarmiganpb.Lease
	CreateLeaseErr error
	RevokeLeaseErr error
}

var errAny = errors.New("")

func normalizeLease(lease ptarmiganpb.Lease) ptarmiganpb.Lease {
	return ptarmiganpb.Lease{
		ID:         lease.ID,
		GrantedTTL: lease.GrantedTTL,
	}
}

func TestStorage(t *testing.T) {
	for _, plugin := range plugins.Plugins() {
		t.Run(fmt.Sprintf("Storage(%s)", plugin.Name()), func(t *testing.T) {
			testStorage(builder(plugin), t)
		})
	}
}

// Tests Follow Structure
// 1) Start With Some Initial State (We Assume That It Is Correct Based On A Series Of Operations Submitted)
// 2) Perform An Operation
// 3) Verify Operation Response Looks Like It Should
// 4) Verify End State Looks Like It Should
// Property-based testing Properties Under Test
// Query:
// Changes:
//

func testStorage(builder tempStoreBuilder, t *testing.T) {
	t.Run("Store", func(t *testing.T) { testStore(builder, t) })
	t.Run("ReplicaStore", func(t *testing.T) { testReplicaStore(builder, t) })
	//t.Run("Update", func(t *testing.T) { testUpdate(builder, t) })
}

func testStore(builder tempStoreBuilder, t *testing.T) {
	t.Run("ReplicaStores", func(t *testing.T) { testStoreReplicaStores(builder, t) })
}

func testStoreReplicaStores(builder tempStoreBuilder, t *testing.T) {
	testCases := map[string]struct {
		replicaStores []string
		start         string
		limit         int
		result        []string
	}{
		"empty": {
			replicaStores: []string{},
			start:         "",
			limit:         -1,
			result:        []string{},
		},
		"empty-non-empty-start": {
			replicaStores: []string{},
			start:         "abc",
			limit:         -1,
			result:        []string{},
		},
		"not-empty-all": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "",
			limit:         -1,
			result:        []string{"abc", "def", "ghi", "jkl"},
		},
		"not-empty-first-half-1": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "",
			limit:         2,
			result:        []string{"abc", "def"},
		},
		"not-empty-first-half-2": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "a",
			limit:         2,
			result:        []string{"abc", "def"},
		},
		"not-empty-last-half-1": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "def",
			limit:         2,
			result:        []string{"ghi", "jkl"},
		},
		"not-empty-last-half-2": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "eee",
			limit:         2,
			result:        []string{"ghi", "jkl"},
		},
		"not-empty-middle-half-1": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "abc",
			limit:         2,
			result:        []string{"def", "ghi"},
		},
		"not-empty-middle-half-2": {
			replicaStores: []string{"abc", "def", "ghi", "jkl"},
			start:         "ccc",
			limit:         2,
			result:        []string{"def", "ghi"},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ptarmiganStore, cleanup := builder(t)
			defer cleanup()

			for i, replicaStore := range testCase.replicaStores {
				ptarmiganStore.ReplicaStore(replicaStore).Create(context.Background(), []byte(fmt.Sprintf("metadata-%d", i)))
			}

			replicaStores, err := ptarmiganStore.ReplicaStores(context.Background(), testCase.start, testCase.limit)

			if err != nil {
				t.Fatalf("expected err to be nil, got %#v", err)
			}

			diff := cmp.Diff(testCase.result, replicaStores)

			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testReplicaStore(builder tempStoreBuilder, t *testing.T) {
	// t.Run("Query", func(t *testing.T) { testReplicaStoreTxnAndQuery(builder, t) })
	// t.Run("Changes", func(t *testing.T) { testReplicaStoreChanges(builder, t) })
	// t.Run("Leases", func(t *testing.T) { testReplicaStoreLeases(builder, t) })
	// t.Run("GetLease", func(t *testing.T) { testReplicaStoreGetLease(builder, t) })
	// t.Run("Snapshot", func(t *testing.T) { testReplicaStoreSnapshot(builder, t) })
	// t.Run("ApplySnapshot", func(t *testing.T) { testReplicaStoreApplySnapshot(builder, t) })

	var genQueryCommand = gen.Const(&commands.ProtoCommand{
		Name: "Query",
		RunFunc: func(q commands.SystemUnderTest) commands.Result {
			req := ptarmiganpb.KVQueryRequest{
				Limit: -1,
			}

			replicaStore := q.(storage.ReplicaStore)
			res, err := replicaStore.Query(context.Background(), req)

			if err != nil {
				panic(err)
			}

			return res
		},
		NextStateFunc: func(state commands.State) commands.State {
			return nil
		},
		PreConditionFunc: func(state commands.State) bool {
			return true
		},
		PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
			resp := ptarmiganpb.KVQueryResponse{
				Kvs: []*ptarmiganpb.KeyValue{
					{
						Key:            []byte("aaa"),
						Value:          []byte("xxx"),
						CreateRevision: 1,
						ModRevision:    1,
						Version:        1,
						Lease:          0,
					},
				},
			}

			diff := cmp.Diff(resp, result)

			if diff != "" {
				fmt.Printf("%s\n", diff)
				return &gopter.PropResult{Status: gopter.PropFalse}
			}

			return &gopter.PropResult{Status: gopter.PropTrue}
		},
	})

	var genTxnCommand = gen.Const(&commands.ProtoCommand{
		Name: "Txn",
		RunFunc: func(q commands.SystemUnderTest) commands.Result {
			req := &ptarmiganpb.KVTxnRequest{
				Success: []*ptarmiganpb.KVRequestOp{
					{
						Request: &ptarmiganpb.KVRequestOp_RequestPut{
							RequestPut: &ptarmiganpb.KVPutRequest{
								Key:   []byte("aaa"),
								Value: []byte("xxx"),
							},
						},
					},
				},
			}

			replicaStore := q.(storage.ReplicaStore)
			index, err := replicaStore.Index(context.Background())

			if err != nil {
				panic(err)
			}

			res, err := replicaStore.Apply(index+1).Txn(context.Background(), *req)

			if err != nil {
				panic(err)
			}

			return res
		},
		NextStateFunc: func(state commands.State) commands.State {
			return nil
		},
		PreConditionFunc: func(state commands.State) bool {
			return true
		},
		PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
			resp := ptarmiganpb.KVTxnResponse{
				Succeeded: true,
				Responses: []*ptarmiganpb.KVResponseOp{
					{
						Response: &ptarmiganpb.KVResponseOp_ResponsePut{
							ResponsePut: &ptarmiganpb.KVPutResponse{},
						},
					},
				},
			}

			diff := cmp.Diff(resp, result)

			if diff != "" {
				fmt.Printf("%s\n", diff)
				return &gopter.PropResult{Status: gopter.PropFalse}
			}

			return &gopter.PropResult{Status: gopter.PropTrue}
		},
	})

	type replicaStoreWithCleanup struct {
		storage.ReplicaStore
		cleanup func()
	}

	var cbCommands = &commands.ProtoCommands{
		NewSystemUnderTestFunc: func(initialState commands.State) commands.SystemUnderTest {
			store, cleanup := builder(t)
			replicaStore := store.ReplicaStore("test")

			if err := replicaStore.Create(context.Background(), []byte{}); err != nil {
				panic(err)
			}

			return &replicaStoreWithCleanup{ReplicaStore: replicaStore, cleanup: cleanup}
		},
		DestroySystemUnderTestFunc: func(sut commands.SystemUnderTest) {
			sut.(*replicaStoreWithCleanup).cleanup()
		},
		InitialStateGen: gopter.CombineGens().Map(func([]interface{}) *ReplicaStore {
			return NewReplicaStore()
		}),
		InitialPreConditionFunc: func(state commands.State) bool {
			return true
		},
		GenCommandFunc: func(state commands.State) gopter.Gen {
			return gen.OneGenOf(genQueryCommand, genTxnCommand)
		},
	}

	parameters := gopter.DefaultTestParametersWithSeed(1234)
	properties := gopter.NewProperties(parameters)
	properties.Property("", commands.Prop(cbCommands))
	properties.TestingRun(t)
}

// func testReplicaStoreTxnAndQuery(builder tempStoreBuilder, t *testing.T) {
// 	testCases := map[string]struct {
// 		transactions         []transaction
// 		transactionResponses []transactionResponse
// 		query                *ptarmiganpb.KVQueryRequest
// 		queryResponse        *ptarmiganpb.KVQueryResponse
// 		queryErr             error
// 		changesQuery         *ptarmiganpb.KVWatchRequest
// 		changesLimit         int
// 		changesResponse      []ptarmiganpb.Event
// 		changesErr           error
// 		leasesResponse       []ptarmiganpb.Lease
// 		leasesErr            error
// 		getLeaseID           *int64
// 		getLeaseResponse     *ptarmiganpb.Lease
// 		getLeaseErr          error
// 	}{
// 		"one-put": {
// 			transactions: []transaction{
// 				{
// 					Txn: &ptarmiganpb.KVTxnRequest{
// 						Success: []*ptarmiganpb.KVRequestOp{
// 							{
// 								Request: &ptarmiganpb.KVRequestOp_RequestPut{
// 									RequestPut: &ptarmiganpb.KVPutRequest{
// 										Key:   []byte("aaa"),
// 										Value: []byte("xxx"),
// 									},
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 			transactionResponses: []transactionResponse{
// 				{
// 					Txn: &ptarmiganpb.KVTxnResponse{
// 						Succeeded: true,
// 						Responses: []*ptarmiganpb.KVResponseOp{
// 							{
// 								Response: &ptarmiganpb.KVResponseOp_ResponsePut{
// 									ResponsePut: &ptarmiganpb.KVPutResponse{},
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 			query: &ptarmiganpb.KVQueryRequest{
// 				Limit: -1,
// 			},
// 			queryResponse: &ptarmiganpb.KVQueryResponse{
// 				Kvs: []*ptarmiganpb.KeyValue{
// 					{
// 						Key:            []byte("aaa"),
// 						Value:          []byte("xxx"),
// 						CreateRevision: 1,
// 						ModRevision:    1,
// 						Version:        1,
// 						Lease:          0,
// 					},
// 				},
// 			},
// 		},
// 	}

// 	for name, testCase := range testCases {
// 		t.Run(name, func(t *testing.T) {
// 			ptarmiganStore := builder(t)

// 			if len(testCase.transactions) != len(testCase.transactionResponses) {
// 				t.Fatalf("expected length of transactions to be same as the length of transaction responses")
// 			}

// 			testStore := ptarmiganStore.ReplicaStore("test")

// 			if err := testStore.Create(context.Background(), []byte(fmt.Sprintf("metadata"))); err != nil {
// 				t.Fatalf("expected err to be nil, got %#v", err)
// 			}

// 			responses := make([]transactionResponse, len(testCase.transactions))
// 			leaseReferenceMap := make(map[int64]int64)

// 			for i, txn := range testCase.transactions {
// 				var resp transactionResponse

// 				if txn.Txn != nil {
// 					txnResp, err := testStore.Apply(uint64(i+1)).Txn(context.Background(), *txn.Txn)
// 					resp.Txn = &txnResp
// 					resp.TxnErr = err
// 				} else if txn.Compact != nil {
// 					resp.CompactErr = testStore.Apply(uint64(i+1)).Compact(context.Background(), *txn.Compact)
// 				} else if txn.CreateLease != nil {
// 					createLeaseResp, err := testStore.Apply(uint64(i+1)).CreateLease(context.Background(), *txn.CreateLease)
// 					if err != nil {
// 						leaseReferenceMap[int64(i)] = createLeaseResp.ID
// 						createLeaseResp = normalizeLease(createLeaseResp)
// 						createLeaseResp.ID = int64(i)
// 					}
// 					resp.CreateLease = &createLeaseResp
// 					resp.CompactErr = err
// 				} else if txn.RevokeLease != nil {
// 					realID, ok := leaseReferenceMap[*txn.RevokeLease]

// 					if !ok {
// 						t.Fatalf("transaction %d is a revoke lease operation that references the lease created in transaction %d, but can't find the id of that lease", i, *txn.RevokeLease)
// 					}

// 					resp.RevokeLeaseErr = testStore.Apply(uint64(i+1)).RevokeLease(context.Background(), realID)
// 				}

// 				responses[i] = resp
// 			}

// 			diff := cmp.Diff(testCase.transactionResponses, responses)

// 			if diff != "" {
// 				t.Fatalf(diff)
// 			}

// 			if testCase.queryResponse != nil {
// 				resp, err := testStore.Query(context.Background(), *testCase.query)

// 				if testCase.queryErr == errAny {
// 					if err != nil {
// 						t.Fatalf("expected err not to be nil, got %#v", err)
// 					}
// 				} else if testCase.queryErr != err {
// 					t.Fatalf("expected err to be %#v, got %#v", testCase.queryErr, err)
// 				}

// 				diff := cmp.Diff(*testCase.queryResponse, resp)

// 				if diff != "" {
// 					t.Fatalf(diff)
// 				}
// 			}

// 			if testCase.changesResponse != nil {
// 				resp, err := testStore.Changes(context.Background(), *testCase.changesQuery, testCase.changesLimit)

// 				if testCase.changesErr == errAny {
// 					if err != nil {
// 						t.Fatalf("expected err not to be nil, got %#v", err)
// 					}
// 				} else if testCase.changesErr != err {
// 					t.Fatalf("expected err to be %#v, got %#v", testCase.changesErr, err)
// 				}

// 				diff := cmp.Diff(testCase.changesResponse, resp)

// 				if diff != "" {
// 					t.Fatalf(diff)
// 				}
// 			}

// 			if testCase.leasesResponse != nil {
// 				resp, err := testStore.Leases(context.Background())

// 				if testCase.leasesErr == errAny {
// 					if err != nil {
// 						t.Fatalf("expected err not to be nil, got %#v", err)
// 					}
// 				} else if testCase.leasesErr != err {
// 					t.Fatalf("expected err to be %#v, got %#v", testCase.leasesErr, err)
// 				}

// 				// Map IDs back to relative indexes and normalize
// 				for i, lease := range resp {
// 					lease = normalizeLease(lease)

// 					for j, realID := range leaseReferenceMap {
// 						if lease.ID == realID {
// 							lease.ID = j
// 							break
// 						}
// 					}

// 					resp[i] = lease
// 				}

// 				diff := cmp.Diff(testCase.leasesResponse, resp)

// 				if diff != "" {
// 					t.Fatalf(diff)
// 				}
// 			}

// 			if testCase.getLeaseResponse != nil {
// 				realID, ok := leaseReferenceMap[*testCase.getLeaseID]

// 				if !ok {
// 					t.Fatalf("get lease references the lease created in transaction %d, but can't find the id of that lease", *testCase.getLeaseID)
// 				}

// 				resp, err := testStore.GetLease(context.Background(), realID)

// 				if testCase.getLeaseErr == errAny {
// 					if err != nil {
// 						t.Fatalf("expected err not to be nil, got %#v", err)
// 					}
// 				} else if testCase.getLeaseErr != err {
// 					t.Fatalf("expected err to be %#v, got %#v", testCase.getLeaseErr, err)
// 				}

// 				if err == nil {
// 					resp = normalizeLease(resp)
// 					resp.ID = *testCase.getLeaseID
// 				}

// 				diff := cmp.Diff(*testCase.getLeaseResponse, resp)

// 				if diff != "" {
// 					t.Fatalf(diff)
// 				}
// 			}
// 		})
// 	}
// }

// func testReplicaStoreChanges(builder tempStoreBuilder, t *testing.T) {
// }

// func testReplicaStoreLeases(builder tempStoreBuilder, t *testing.T) {
// }

// func testReplicaStoreGetLease(builder tempStoreBuilder, t *testing.T) {
// }

// func testReplicaStoreSnapshot(builder tempStoreBuilder, t *testing.T) {
// }

// func testReplicaStoreApplySnapshot(builder tempStoreBuilder, t *testing.T) {
// }

// func testUpdate(builder tempStoreBuilder, t *testing.T) {
// 	t.Run("Txn", func(t *testing.T) { testUpdateTxn(builder, t) })
// 	t.Run("Compact", func(t *testing.T) { testUpdateCompact(builder, t) })
// 	t.Run("CreateLease", func(t *testing.T) { testUpdateCreateLease(builder, t) })
// 	t.Run("RevokeLease", func(t *testing.T) { testUpdateRevokeLease(builder, t) })
// }

// func testUpdateTxn(builder tempStoreBuilder, t *testing.T) {
// }

// func testUpdateCompact(builder tempStoreBuilder, t *testing.T) {
// }

// func testUpdateCreateLease(builder tempStoreBuilder, t *testing.T) {
// }

// func testUpdateRevokeLease(builder tempStoreBuilder, t *testing.T) {
// }
