package model

import (
	"github.com/zoyi/bingodb/ds/skiplist"
	"sync"
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
	listData, _ := index.Data.Load(hash)

	if list, ok := listData.(*skiplist.SkipList); ok {
		if list.Len() == 0 {
			return result, next
		}

		var start interface{} = nil
		var end interface{} = nil

		if startSortKey != nil {
			start = index.SortKey.Parse(startSortKey)
		} else {
			start = list.SeekToFirst()
		}
		if endSortKey != nil {
			end = index.SortKey.Parse(endSortKey)
		} else {
			end = list.SeekToLast()
		}

		//list.Range(startSortKey, endSortKey)
		//if tree.Comparator(start, end) > 0 {
		//	return result, next
		//}

		boundIterator := list.Range(start, end)
		for boundIterator.Next() {
			if len(result) == limit {
				next = boundIterator.Key()
				break
			}
			result = append(result, boundIterator.Value().(*Document))
		}

		//for it := tree.Find(start); it.Present() && len(result) <= limit && tree.Comparator(it.Key(), end) <= 0; it.Next() {
		//	if len(result) == limit {
		//		next = it.Key()
		//		break
		//	}
		//	result = append(result, it.Value().(*Document))
		//}
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
	listData, _ := index.Data.Load(hash)

	if list, ok := listData.(*skiplist.SkipList); ok {
		//if tree.Size() == 0 {
		//	return result, next
		//}

		if list.Len() == 0 {
			return result, next
		}

		var start interface{} = nil
		var end interface{} = nil

		if startSortKey != nil {
			start = index.SortKey.Parse(startSortKey)
		} else {
			start = list.SeekToLast()
		}
		if endSortKey != nil {
			end = index.SortKey.Parse(endSortKey)
		} else {
			end = list.SeekToFirst()
		}

		boundIterator := list.Range(start, end)
		boundIterator.Seek(end)
		for boundIterator.Previous() {
			if len(result) == limit {
				next = boundIterator.Key()
				break
			}
			result = append(result, boundIterator.Value().(*Document))
		}

		//if tree.Comparator(start, end) > 0 {
		//	return result, next
		//}
		//
		//for it := tree.RFind(end); it.Present() && len(result) <= limit && tree.Comparator(start, it.Key()) <= 0; it.Next() {
		//	if len(result) == limit {
		//		next = it.Key()
		//		break
		//	}
		//	result = append(result, it.Value().(*Document))
		//}
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
	listData, _ := index.Data.Load(hash)

	if list, ok := listData.(*skiplist.SkipList); ok {
		//if tree.Size() == 0 {
		//	return result, next
		//}
		if list.Len() == 0 {
			return result, next
		}

		if startSortKey.Empty() {
			startSortKey = list.SeekToFirst().Key().(SubSortTreeKey)
		}
		if endSortKey.Empty() {
			endSortKey = list.SeekToLast().Key().(SubSortTreeKey)
		}

		boundIterator := list.Range(startSortKey, endSortKey)
		for boundIterator.Next() {
			if len(result) == limit {
				next = boundIterator.Key().(SubSortTreeKey)
				break
			}
			result = append(result, boundIterator.Key().(SubSortTreeKey).Document)
		}
		//if tree.Comparator(startSortKey, endSortKey) > 0 {
		//	return result, next
		//}
		//
		//for it := tree.Find(startSortKey); it.Present() && len(result) <= limit && tree.Comparator(it.Key(), endSortKey) <= 0; it.Next() {
		//	if len(result) == limit {
		//		next = it.Key().(SubSortTreeKey)
		//		break
		//	}
		//	result = append(result, it.Key().(SubSortTreeKey).Document)
		//}
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
	listData, _ := index.Data.Load(hash)

	if list, ok := listData.(*skiplist.SkipList); ok {
		if list.Len() == 0 {
			return result, next
		}

		if startSortKey.Empty() {
			startSortKey = list.SeekToFirst().Key().(SubSortTreeKey)
		}
		if endSortKey.Empty() {
			endSortKey = list.SeekToLast().Key().(SubSortTreeKey)
		}

		boundIterator := list.Range(startSortKey, endSortKey)
		boundIterator.Seek(endSortKey)
		for boundIterator.Previous() {
			if len(result) == limit {
				next = boundIterator.Key().(SubSortTreeKey)
				break
			}
			result = append(result, boundIterator.Key().(SubSortTreeKey).Document)
		}
		//if tree.Comparator(startSortKey, endSortKey) > 0 {
		//	return result, next
		//}
		//
		//for it := tree.RFind(endSortKey); it.Present() && len(result) <= limit && tree.Comparator(startSortKey, it.Key()) <= 0; it.Next() {
		//	if len(result) == limit {
		//		next = it.Key().(SubSortTreeKey)
		//		break
		//	}
		//	result = append(result, it.Key().(SubSortTreeKey).Document)
		//}
	}

	return result, next
}

func (index *PrimaryIndex) delete(
	hash interface{},
	sort interface{}) (*Document, bool) {

	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	listData, _ := index.Data.Load(hash)

	if list, ok := listData.(*skiplist.SkipList); ok {
		value, present := list.Delete(sort)
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

	listData, _ := index.Data.Load(hash)

	if list, ok := listData.(*skiplist.SkipList); ok {
		list.Delete(sort)
	}
}

func (index *PrimaryIndex) Get(
	hash interface{},
	sort interface{}) (*Document, bool) {

	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	treeData, _ := index.Data.Load(hash)

	if list, ok := treeData.(*skiplist.SkipList); ok {
		value, present := list.Get(sort)
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

	listData, _ := index.Data.Load(hash)

	if list, ok := listData.(*skiplist.SkipList); ok {
		value, present := list.Get(SubSortTreeKey{Key: sort})
		//node, present := tree.Ceiling(SubSortTreeKey{Key: sort})
		if !present || value != sort {
			return nil, false
		}

		return value.(*Document), true
	}

	return nil, false
}

func (index *PrimaryIndex) put(doc *Document) (*Document, bool) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := doc.Get(index.SortKey.Name)

	var list *skiplist.SkipList
	//var tree *redblacktree.Tree
	var ok bool
	listData, ok := index.Data.Load(hashValue)

	if !ok {
		list = skiplist.NewStringMap()
		//tree = redblacktree.NewWithStringComparator()
		index.Data.Store(hashValue, list)
	} else {
		list, _ = listData.(*skiplist.SkipList)
	}

	list.Set(sortValue, doc)
	return doc, true

	//removed, replaced := tree.Put(sortValue, doc)
	//if replaced {
	//	return removed.(*Document), true
	//} else {
	//	return nil, false
	//}
}

func (index *SubIndex) put(doc *Document) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := SubSortTreeKey{Key: doc.Get(index.SortKey.Name), Document: doc}

	var list *skiplist.SkipList
	//var tree *redblacktree.Tree
	var ok bool

	listData, _ := index.Data.Load(hashValue)

	if list, ok = listData.(*skiplist.SkipList); !ok {
		list = skiplist.NewCustomMap(func(l, r interface{}) bool {
			ka := l.(SubSortTreeKey)
			kb := r.(SubSortTreeKey)
			o := GeneralCompare(ka.Key, kb.Key)
			if o != 0 {
				return o < 0
			}

			return doc.schema.PrimaryKey.Compare(ka.Document, kb.Document) < 0
		})

		//&redblacktree.Tree{Comparator: func(a, b interface{}) int {
		//ka := a.(SubSortTreeKey)
		//kb := b.(SubSortTreeKey)
		//o := GeneralCompare(ka.Key, kb.Key)
		//if o != 0 {
		//	return o
		//}
		//
		//return doc.schema.PrimaryKey.Compare(ka.Document, kb.Document)
		//},
	}

	index.Data.Store(hashValue, list)
	list.Set(sortValue, nil)
	//tree.Put(sortValue, nil)
}
