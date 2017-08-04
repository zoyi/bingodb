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
			start = list.SeekToFirst().Key()
		}
		if endSortKey != nil {
			end = index.SortKey.Parse(endSortKey)
		} else {
			end = list.SeekToLast().Key()
		}

		//greater or equal - less than
		boundIterator := list.Seek(start)
		if boundIterator == nil || 0 >= limit {
			return result, next
		}
		//add first element if limit is greater than 0
		result = append(result, boundIterator.Value().(*Document))
		for boundIterator.Next() {
			lessThanOrNotEqual := !list.LessThan(boundIterator.Key(), end) && boundIterator.Key() != end
			if len(result) == limit || lessThanOrNotEqual {
				if boundIterator.Next() {
					next = boundIterator.Key()
				}
				break
			}
			result = append(result, boundIterator.Value().(*Document))
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
			start = list.SeekToLast().Key()
		}
		if endSortKey != nil {
			end = index.SortKey.Parse(endSortKey)
		} else {
			end = list.SeekToFirst().Key()
		}

		//greater or equal - less than
		boundIterator := list.Seek(start)
		if boundIterator == nil || 0 >= limit  {
			return result, next
		}
		//add first element if limit is greater than 0
		result = append(result, boundIterator.Value().(*Document))
		for boundIterator.Previous() {
			greaterThanOrNotEqual := list.LessThan(boundIterator.Key(), end) && boundIterator.Key() != end
			if len(result) == limit || greaterThanOrNotEqual {
				if boundIterator.Previous() {
					next = boundIterator.Key()
				}
				break
			}
			result = append(result, boundIterator.Value().(*Document))
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

		//greater or equal - less than
		boundIterator := list.Seek(startSortKey)
		if boundIterator == nil || 0 >= limit{
			return result, next
		}
		//add first element
		result = append(result, boundIterator.Key().(SubSortTreeKey).Document)
		for boundIterator.Next() {
			lessThanOrNotEqual := !list.LessThan(boundIterator.Key().(SubSortTreeKey), endSortKey) &&
				boundIterator.Key() != endSortKey
			if len(result) == limit || lessThanOrNotEqual {
				if boundIterator.Next() {
					next = boundIterator.Key().(SubSortTreeKey)
				}
				break
			}
			result = append(result, boundIterator.Key().(SubSortTreeKey).Document)
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

		//greater or equal - less than
		boundIterator := list.Seek(startSortKey)
		if boundIterator == nil || 0 >= limit {
			return result, next
		}
		//add first element if limit is greater than 0
		result = append(result, boundIterator.Key().(SubSortTreeKey).Document)
		for boundIterator.Previous() {
			greaterThanOrNotEqual := list.LessThan(boundIterator.Key().(SubSortTreeKey), endSortKey) &&
				boundIterator.Key() != endSortKey
			if len(result) == limit || greaterThanOrNotEqual {
				if boundIterator.Previous() {
					next = boundIterator.Key().(SubSortTreeKey)
				}
				break
			}
			result = append(result, boundIterator.Key().(SubSortTreeKey).Document)
		}
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
		if !present || value != sort {
			return nil, false
		}

		return value.(*Document), true
	}

	return nil, false
}

func (index *PrimaryIndex) put(doc *Document) *Document {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := doc.Get(index.SortKey.Name)

	var list *skiplist.SkipList
	var ok bool
	listData, ok := index.Data.Load(hashValue)

	if !ok {
		list = skiplist.NewStringMap()
		index.Data.Store(hashValue, list)
	} else {
		list, _ = listData.(*skiplist.SkipList)
	}

	list.Set(sortValue, doc)
	return doc
}

func (index *SubIndex) put(doc *Document) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := SubSortTreeKey{Key: doc.Get(index.SortKey.Name), Document: doc}

	var list *skiplist.SkipList
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
	}

	index.Data.Store(hashValue, list)
	list.Set(sortValue, nil)
}
