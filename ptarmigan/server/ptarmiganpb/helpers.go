package ptarmiganpb

var (
	emptyPredicate   = KVPredicate{}
	emptyWatchCursor = KVWatchCursor{}
)

// IsEmpty returns true is this predicate is empty
func (predicate KVPredicate) IsEmpty() bool {
	return predicate == emptyPredicate
}
