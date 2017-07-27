package model

import (
	"sync"
	"github.com/zoyi/bingodb/ds/redblacktree"
)

type Index struct {
	Data *sync.Map
	*Key
}

type PrimaryIndex struct {
	*Index
}

type SubIndex struct {
	*Index
}

func (index *PrimaryIndex) Fetch(
	hash interface{},
	startSortKey interface{},
	endSortKey interface{},
	limit int) ([](*Document), interface{}) {
	var result [](*Document)
	var next interface{} = nil

	hash = index.HashKey.Parse(hash)
	treeData, _ := index.Data.Load(hash)

	if tree, ok := treeData.(*redblacktree.Tree); ok {
		if tree.Size() == 0 {
			return result, next
		}

		var start interface{} = nil
		var end interface{} = nil

		if startSortKey != nil {
			start = index.SortKey.Parse(startSortKey)
		} else {
			start = tree.Left().Key
		}
		if endSortKey != nil {
			end = index.SortKey.Parse(endSortKey)
		} else {
			end = tree.Right().Key
		}

		if tree.Comparator(start, end) > 0 {
			return result, next
		}

		for it := tree.Find(start);
			it.Present() && len(result) <= limit && tree.Comparator(it.Key(), end) <= 0; it.Next() {
			if len(result) == limit {
				next = it.Key()
				break
			}
			result = append(result, it.Value().(*Document))
		}
	}

	return result, next
}

func (index *PrimaryIndex) RFetch(
	hash interface{},
	startSortKey interface{},
	endSortKey interface{},
	limit int) ([](*Document), interface{}) {

	var result [](*Document)
	var next interface{} = nil

	hash = index.HashKey.Parse(hash)
	treeData, _ := index.Data.Load(hash)

	if tree, ok := treeData.(*redblacktree.Tree); ok {
		if tree.Size() == 0 {
			return result, next
		}

		var start interface{} = nil
		var end interface{} = nil

		if startSortKey != nil {
			start = index.SortKey.Parse(startSortKey)
		} else {
			start = tree.Left().Key
		}
		if endSortKey != nil {
			end = index.SortKey.Parse(endSortKey)
		} else {
			end = tree.Right().Key
		}

		if tree.Comparator(start, end) > 0 {
			return result, next
		}

		for it := tree.RFind(end);
			it.Present() && len(result) <= limit && tree.Comparator(start, it.Key()) <= 0; it.Next() {
			if len(result) == limit {
				next = it.Key()
				break
			}
			result = append(result, it.Value().(*Document))
		}
	}

	return result, next
}

func (index *SubIndex) Fetch(
	hash interface{},
	startSortKey SubSortTreeKey,
	endSortKey SubSortTreeKey,
	limit int) ([](*Document), SubSortTreeKey) {
	var result [](*Document)
	var next SubSortTreeKey

	hash = index.HashKey.Parse(hash)
	treeData, _ := index.Data.Load(hash)

	if tree, ok := treeData.(*redblacktree.Tree); ok {
		if tree.Size() == 0 {
			return result, next
		}

		if startSortKey.Empty() {
			startSortKey = tree.Left().Key.(SubSortTreeKey)
		}
		if endSortKey.Empty() {
			endSortKey = tree.Right().Key.(SubSortTreeKey)
		}

		if tree.Comparator(startSortKey, endSortKey) > 0 {
			return result, next
		}

		for it := tree.Find(startSortKey);
			it.Present() && len(result) <= limit && tree.Comparator(it.Key(), endSortKey) <= 0; it.Next() {
			if len(result) == limit {
				next = it.Key().(SubSortTreeKey)
				break
			}
			result = append(result, it.Key().(SubSortTreeKey).Document)
		}
	}
	return result, next
}

func (index *SubIndex) RFetch(
	hash interface{},
	startSortKey SubSortTreeKey,
	endSortKey SubSortTreeKey,
	limit int) ([](*Document), SubSortTreeKey) {

	var result [](*Document)
	var next SubSortTreeKey

	hash = index.HashKey.Parse(hash)
	treeData, _ := index.Data.Load(hash)

	if tree, ok := treeData.(*redblacktree.Tree); ok {
		if tree.Size() == 0 {
			return result, next
		}

		if startSortKey.Empty() {
			startSortKey = tree.Left().Key.(SubSortTreeKey)
		}
		if endSortKey.Empty() {
			endSortKey = tree.Right().Key.(SubSortTreeKey)
		}

		if tree.Comparator(startSortKey, endSortKey) > 0 {
			return result, next
		}

		for it := tree.RFind(endSortKey); it.Present() && len(result) <= limit && tree.Comparator(startSortKey, it.Key()) <= 0; it.Next() {
			if len(result) == limit {
				next = it.Key().(SubSortTreeKey)
				break
			}
			result = append(result, it.Key().(SubSortTreeKey).Document)
		}
	}

	return result, next
}

func (index *PrimaryIndex) delete(
	hash interface{},
	sort interface{}) (*Document, bool) {

	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	treeData, _ := index.Data.Load(hash)

	if tree, ok := treeData.(*redblacktree.Tree); ok {
		value, present := tree.Remove(sort)
		if !present {
			return nil, false
		}

		return value.(*Document), true
	}
	return nil, false
}

func (index *SubIndex) delete(doc *Document) {
	hash := doc.Get(index.HashKey.Name)
	sort := SubSortTreeKey{Key: doc.Get(index.SortKey.Name), Document: doc}

	treeData, _ := index.Data.Load(hash)

	if tree, ok := treeData.(*redblacktree.Tree); ok {
		tree.Remove(sort)
	}
}

func (index *PrimaryIndex) Get(
	hash interface{},
	sort interface{}) (*Document, bool) {

	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	treeData, _ := index.Data.Load(hash)

	if tree, ok := treeData.(*redblacktree.Tree); ok {
		value, present := tree.Get(sort)
		if !present {
			return nil, false
		}

		return value.(*Document), true
	}

	return nil, false
}

func (index *SubIndex) Get(
	hash interface{},
	sort interface{}) (*Document, bool) {

	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	treeData, _ := index.Data.Load(hash)

	if tree, ok := treeData.(*redblacktree.Tree); ok {
		node, present := tree.Ceiling(SubSortTreeKey{Key: sort})
		if !present || node.Key.(SubSortTreeKey).Key != sort {
			return nil, false
		}

		return node.Key.(SubSortTreeKey).Document, true
	}

	return nil, false
}

func (index *PrimaryIndex) put(doc *Document) (*Document, bool) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := doc.Get(index.SortKey.Name)

	var tree *redblacktree.Tree
	var ok bool
	treeData, ok := index.Data.Load(hashValue)

	if !ok {
		tree = redblacktree.NewWithStringComparator()
		index.Data.Store(hashValue, tree)
	} else if tree, ok = treeData.(*redblacktree.Tree); !ok {
		return nil, false
	}

	removed, replaced := tree.Put(sortValue, doc)
	if replaced {
		return removed.(*Document), true
	} else {
		return nil, false
	}
}

func (index *SubIndex) put(doc *Document) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := SubSortTreeKey{Key: doc.Get(index.SortKey.Name), Document: doc}

	var tree *redblacktree.Tree
	var ok bool

	treeData, _ := index.Data.Load(hashValue)

	if tree, ok = treeData.(*redblacktree.Tree); !ok {
		tree = &redblacktree.Tree{Comparator: func(a, b interface{}) int {
			ka := a.(SubSortTreeKey)
			kb := b.(SubSortTreeKey)
			o := GeneralCompare(ka.Key, kb.Key)
			if o != 0 {
				return o
			}

			return doc.schema.PrimaryKey.Compare(ka.Document, kb.Document)
		},
		}
		index.Data.Store(hashValue, tree)
	}

	tree.Put(sortValue, nil)
}
