package bingodb

import (
	"github.com/zoyi/skiplist/lazy"
	"sync"
	"sync/atomic"
)

type IndexInterface interface {
	Get(hash interface{}, sort interface{}) (*Document, bool)
	Scan(hash interface{}, since interface{}, limit int) (result []Data, next interface{})
}

type index struct {
	m       *sync.Map
	hashKey *FieldSchema
	sortKey *FieldSchema
	size    int64
}

type PrimaryIndex struct {
	IndexInterface
	*index
}

type SubIndex struct {
	IndexInterface
	*index
	primarySortKey *FieldSchema
}

func newIndex(hashKey *FieldSchema, sortKey *FieldSchema) *index {
	return &index{m: new(sync.Map), hashKey: hashKey, sortKey: sortKey}
}

func (index *index) skipList(hash interface{}) *lazyskiplist.SkipList {
	if read, ok := index.m.Load(hash); ok {
		return read.(*lazyskiplist.SkipList)
	}
	return nil
}

func (index *PrimaryIndex) Range(f func(key interface{}, list *lazyskiplist.SkipList) bool) {
	index.m.Range(func(key, value interface{}) bool {
		return f(key, value.(*lazyskiplist.SkipList))
	})
}

func (index *PrimaryIndex) Scan(hash interface{}, since interface{}, limit int) (result []Data, next interface{}) {
	hash = index.hashKey.Parse(hash)
	since = index.parseSortKey(since)

	if list := index.skipList(hash); list != nil {
		it := list.Begin(since)
		for i := 0; i < limit && it.Present(); i, _ = i+1, it.Next() {
			result = append(result, it.Value().(*Document).GetData())
		}
		if it.Present() {
			next = it.Key()
		}
	}
	return result, next
}

func (index *PrimaryIndex) RScan(hash interface{}, since interface{}, limit int) (result []Data, next interface{}) {
	hash = index.hashKey.Parse(hash)
	since = index.parseSortKey(since)

	if list := index.skipList(hash); list != nil {
		it := list.End(since)
		for i := 0; i < limit && it.Present(); i, _ = i+1, it.Prev() {
			result = append(result, it.Value().(*Document).GetData())
		}
		if it.Present() {
			next = it.Key()
		}
	}
	return result, next
}

func (index *PrimaryIndex) Get(hash interface{}, sort interface{}) (*Document, bool) {
	hash = index.hashKey.Parse(hash)
	sort = index.parseSortKey(sort)

	if list := index.skipList(hash); list != nil {
		if value, ok := list.Get(sort); ok {
			return value.(*Document), true
		}
	}
	return nil, false
}

//func (index *SubIndex) Get(hash interface{}, sort interface{}) (*Document, bool) {
//	hash = index.schema.hashKey.Parse(hash)
//	sort = index.sortKey.Parse(sort)
//
//	if list := index.skipList(hash); list != nil {
//		if key, value, ok := list.Ceiling(SubSortKey{main: sort}); ok {
//			sortKey := key.(SubSortKey)
//			if sortKey.main == sort {
//				return sortKey.Document, true
//			}
//		}
//	}
//	return nil, false
//}

func (index *SubIndex) Scan(hash interface{}, since interface{}, limit int) (result []Data, next interface{}) {
	hash = index.hashKey.Parse(hash)
	since = index.parseSubSortKey(since)

	if list := index.skipList(hash); list != nil {
		it := list.Begin(since)
		for i := 0; i < limit && it.Present(); i, _ = i+1, it.Next() {
			result = append(result, it.Value().(*Document).GetData())
		}
		if it.Present() {
			next = it.Key()
		}
	}
	return result, next
}

func (index *index) parseSortKey(raw interface{}) interface{} {
	if raw == nil || index.sortKey == nil {
		return nil
	} else {
		return index.sortKey.Parse(raw)
	}
}

func (index *SubIndex) parseSubSortKey(raw interface{}) interface{} {
	if raw == nil || index.sortKey == nil || index.primarySortKey == nil {
		return nil
	}

	switch raw.(type) {
	case SubSortKey:
		return raw

	case []interface{}:
		key := SubSortKey{}
		ary := raw.([]interface{})
		if len(ary) > 0 {
			key.main = index.sortKey.Parse(ary[0])
		}
		if len(ary) > 1 {
			key.sub = index.primarySortKey.Parse(ary[1])
		}
		return key

	default:
		return SubSortKey{main: index.sortKey.Parse(raw)}
	}
}

func (index *index) sortValueFromDoc(doc *Document) interface{} {
	if sortKey := index.sortKey; sortKey != nil {
		return doc.data[sortKey.Name]
	}
	return nil
}

func (index *PrimaryIndex) put(doc *Document) (*Document, bool) {
	hashValue := doc.Get(index.hashKey)
	sortValue := doc.Get(index.sortKey)

	newSkipList := lazyskiplist.NewLazySkipList(GeneralCompare)
	read, _ := index.m.LoadOrStore(hashValue, newSkipList)
	list := read.(*lazyskiplist.SkipList)

	if original, replaced := list.Put(sortValue, doc); replaced {
		return original.(*Document), true
	} else {
		atomic.AddInt64(&index.size, 1)
		return nil, false
	}
}

func (index *SubIndex) put(doc *Document) {
	hashValue := doc.Get(index.hashKey)
	sortValue := SubSortKey{main: doc.Get(index.sortKey), sub: doc.Get(index.primarySortKey)}

	newSkipList := lazyskiplist.NewLazySkipList(func(a, b interface{}) int {
		ka := a.(SubSortKey)
		kb := b.(SubSortKey)
		o := GeneralCompare(ka.main, kb.main)
		if o != 0 {
			return o
		}
		return GeneralCompare(ka.sub, kb.sub)
	})
	read, _ := index.m.LoadOrStore(hashValue, newSkipList)
	list := read.(*lazyskiplist.SkipList)

	if _, replaced := list.Put(sortValue, doc); !replaced {
		atomic.AddInt64(&index.size, 1)
	}
}

func (index *PrimaryIndex) remove(hash interface{}, sort interface{}) (*Document, bool) {
	hash = index.hashKey.Parse(hash)
	sort = index.parseSortKey(sort)

	if list := index.skipList(hash); list != nil {
		if value, ok := list.Remove(sort); ok {
			atomic.AddInt64(&index.size, -1)
			return value.(*Document), true
		}
	}
	return nil, false
}

func (index *SubIndex) remove(doc *Document) {
	hash := doc.Get(index.hashKey)
	sort := SubSortKey{main: doc.Get(index.sortKey), sub: doc.Get(index.primarySortKey)}

	read, _ := index.m.Load(hash)
	list := read.(*lazyskiplist.SkipList)

	if _, ok := list.Remove(sort); ok {
		atomic.AddInt64(&index.size, -1)
	}
}
