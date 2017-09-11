package bingodb

import (
	"github.com/zoyi/skiplist/lazy"
	"sync"
	"sync/atomic"
)

type IndexInterface interface {
	Get(hash interface{}, sort interface{}) (*Document, bool)
	Scan(hash interface{}, since interface{}, limit int) (values []Data, next interface{})
	HashKey() *FieldSchema
	SortKey() *FieldSchema
}

type index struct {
	m *sync.Map
	*Key
	size int64
}

type PrimaryIndex struct {
	IndexInterface
	*index
}

type SubIndex struct {
	IndexInterface
	*index
	primaryKey *Key
}

func newIndex(key *Key) *index {
	return &index{m: new(sync.Map), Key: key}
}

func (index *index) skipList(hash interface{}) *lazyskiplist.SkipList {
	if read, ok := index.m.Load(hash); ok {
		return read.(*lazyskiplist.SkipList)
	}
	return nil
}

func (index *PrimaryIndex) HashKey() *FieldSchema {
	return index.hashKey
}

func (index *PrimaryIndex) SortKey() *FieldSchema {
	return index.sortKey
}

func (index *SubIndex) HashKey() *FieldSchema {
	return index.hashKey
}

func (index *SubIndex) SortKey() *FieldSchema {
	return index.sortKey
}

func (index *PrimaryIndex) Range(f func(key interface{}, list *lazyskiplist.SkipList) bool) {
	index.m.Range(func(key, value interface{}) bool {
		return f(key, value.(*lazyskiplist.SkipList))
	})
}

func (index *PrimaryIndex) Scan(hash interface{}, since interface{}, limit int) (result []Data, next interface{}) {
	hash = ParseField(index.hashKey, hash)
	since = ParseField(index.sortKey, since)

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
	hash = ParseField(index.hashKey, hash)
	since = ParseField(index.sortKey, since)

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
	hash = ParseField(index.hashKey, hash)
	sort = ParseField(index.sortKey, sort)

	if list := index.skipList(hash); list != nil {
		if value, ok := list.Get(sort); ok {
			return value.(*Document), true
		}
	}
	return nil, false
}

func (index *SubIndex) Scan(hash interface{}, since interface{}, limit int) (result []Data, next interface{}) {
	hash = ParseField(index.hashKey, hash)
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

func (index *SubIndex) parseSubSortKey(raw interface{}) interface{} {
	if raw == nil {
		return nil
	}

	switch raw.(type) {
	case SubSortKey:
		return raw

	case []interface{}:
		key := SubSortKey{}
		ary := raw.([]interface{})
		if len(ary) > 0 {
			key.sort = ParseField(index.sortKey, ary[0])
		}
		if len(ary) > 1 {
			key.primaryHash = ParseField(index.primaryKey.hashKey, ary[1])
		}
		if len(ary) > 2 {
			key.primarySort = ParseField(index.primaryKey.sortKey, ary[2])
		}
		return key

	default:
		return SubSortKey{sort: ParseField(index.sortKey, raw)}
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
	hash := doc.Get(index.hashKey)
	sort := index.makeSubSortKey(doc)

	newSkipList := lazyskiplist.NewLazySkipList(func(a, b interface{}) int {
		ka := a.(SubSortKey)
		kb := b.(SubSortKey)
		if res := GeneralCompare(ka.sort, kb.sort); res != 0 {
			return res
		}
		if res := GeneralCompare(ka.primaryHash, kb.primaryHash); res != 0 {
			return res
		}
		return GeneralCompare(ka.primarySort, kb.primarySort)
	})
	read, _ := index.m.LoadOrStore(hash, newSkipList)
	list := read.(*lazyskiplist.SkipList)

	if _, replaced := list.Put(sort, doc); !replaced {
		atomic.AddInt64(&index.size, 1)
	}
}

func (index *PrimaryIndex) remove(hash interface{}, sort interface{}) (*Document, bool) {
	hash = ParseField(index.hashKey, hash)
	sort = ParseField(index.sortKey, sort)

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
	sort := index.makeSubSortKey(doc)

	read, _ := index.m.Load(hash)
	list := read.(*lazyskiplist.SkipList)

	if _, ok := list.Remove(sort); ok {
		atomic.AddInt64(&index.size, -1)
	}
}

func (index *SubIndex) makeSubSortKey(doc *Document) SubSortKey {
	return SubSortKey{
		sort:        doc.Get(index.sortKey),
		primarySort: doc.Get(index.primaryKey.sortKey),
		primaryHash: doc.Get(index.primaryKey.hashKey),
	}
}
