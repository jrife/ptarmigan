package mvcc

import (
	"github.com/jrife/flock/storage/kv"
	"github.com/jrife/flock/storage/kv/keys"
)

// NamespaceView returns a view that will prefix
// all keys with ns
func NamespaceView(view View, ns []byte) View {
	if len(ns) == 0 {
		return view
	}

	return &namespacedView{MapReader: kv.NamespaceMapReader(view, ns), view: view, ns: ns}
}

var _ (View) = (*namespacedView)(nil)

type namespacedView struct {
	kv.MapReader
	view View
	ns   []byte
}

func (nsView *namespacedView) Changes(keys keys.Range, includePrev bool) (DiffIterator, error) {
	iterator, err := nsView.view.Changes(keys.Namespace(nsView.ns), includePrev)

	if err != nil {
		return nil, err
	}

	return &namespacedDiffIterator{DiffIterator: iterator, ns: nsView.ns}, nil
}

func (nsView *namespacedView) Revision() int64 {
	return nsView.view.Revision()
}

type namespacedDiffIterator struct {
	DiffIterator
	key keys.Key
	ns  keys.Key
}

func (nsIter *namespacedDiffIterator) Next() bool {
	if !nsIter.DiffIterator.Next() {
		nsIter.key = nil

		return false
	}

	// strip the namespace prefix
	nsIter.key = nsIter.DiffIterator.Key()[len(nsIter.ns):]

	return true
}

func (nsIter *namespacedDiffIterator) Key() []byte {
	return nsIter.key
}

// NamespaceRevision returns a revision that will
// prefix all keys with ns
func NamespaceRevision(revision Revision, ns []byte) Revision {
	if len(ns) == 0 {
		return revision
	}

	return &namespacedRevision{MapUpdater: kv.NamespaceMapUpdater(revision, ns), View: NamespaceView(revision, ns)}
}

var _ (Revision) = (*namespacedRevision)(nil)

type namespacedRevision struct {
	kv.MapUpdater
	View
}
