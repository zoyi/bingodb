package bingodb

import (
	"errors"
	"github.com/zoyi/skiplist/lazy"
	"sync"
	"sync/atomic"
)

type IndexInterface interface {
	Get(hash interface{}, sort interface{}) (*Document, error)
	Scan(hash interface{}, since interface{}, limit int) (values []Data, next interface{}, err error)
	RScan(hash interface{}, since interface{}, limit int) (values []Data, next interface{}, err error)
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

func (index *PrimaryIndex) parseKeys(hashRaw, sortRaw interface{}) (interface{}, interface{}, error) {
	hash := ParseField(index.hashKey, hashRaw)
	sort := ParseField(index.sortKey, sortRaw)

	if hash == nil {
		return nil, nil, errors.New(HashKeyMissing)
	}

	if sort == nil {
		return nil, nil, errors.New(SortKeyMising)
	}

	return hash, sort, nil
}

func (index *SubIndex) parseKeys(hashRaw, sortRaw interface{}) (interface{}, SubSortKey, error) {
	hash := ParseField(index.hashKey, hashRaw)
	sort := index.parseSubSortKey(sortRaw)

	if hash == nil {
		return nil, sort, errors.New(HashKeyMissing)
	}
	if index.sortKey != nil && sort.sort == nil {
		return nil, sort, errors.New(SortKeyMising)
	}

	return hash, sort, nil
}

func (index *PrimaryIndex) Get(hashRaw, sortRaw interface{}) (*Document, error) {
	hash, sort, err := index.parseKeys(hashRaw, sortRaw)
	if err != nil {
		return nil, err
	}

	if list := index.skipList(hash); list != nil {
		if value, ok := list.Get(sort); ok {
			return value.(*Document), nil
		}
	}
	return nil, errors.New(DocumentNotFound)
}

func (index *PrimaryIndex) Range(f func(key interface{}, list *lazyskiplist.SkipList) bool) {
	index.m.Range(func(key, value interface{}) bool {
		return f(key, value.(*lazyskiplist.SkipList))
	})
}

func (index *PrimaryIndex) Scan(hashRaw, sinceRaw interface{}, limit int) (result []Data, next interface{}, err error) {
	result = make([]Data, 0)
	hash := ParseField(index.hashKey, hashRaw)
	since := ParseField(index.sortKey, sinceRaw)
	if hash == nil {
		return result, next, errors.New(HashKeyMissing)
	}

	if list := index.skipList(hash); list != nil {
		it := list.Begin(since)
		for i := 0; i < limit && it.Present(); i, _ = i+1, it.Next() {
			result = append(result, it.Value().(*Document).Data())
		}
		if it.Present() {
			next = it.Key()
		}
	}
	return result, next, nil
}

func (index *PrimaryIndex) RScan(hashRaw, sinceRaw interface{}, limit int) (result []Data, next interface{}, err error) {
	result = make([]Data, 0)
	hash := ParseField(index.hashKey, hashRaw)
	since := ParseField(index.sortKey, sinceRaw)
	if hash == nil {
		return result, next, errors.New(HashKeyMissing)
	}

	if list := index.skipList(hash); list != nil {
		it := list.End(since)
		for i := 0; i < limit && it.Present(); i, _ = i+1, it.Prev() {
			result = append(result, it.Value().(*Document).Data())
		}
		if it.Present() {
			next = it.Key()
		}
	}
	return result, next, nil
}

func (index *SubIndex) Get(hashRaw, sortRaw interface{}) (*Document, error) {
	hash, sort, err := index.parseKeys(hashRaw, sortRaw)
	if err != nil {
		return nil, err
	}

	if list := index.skipList(hash); list != nil {
		if key, value, ok := list.Ceiling(sort); ok {
			if key != nil && GeneralCompare(sort.sort, key.(SubSortKey).sort) == 0 {
				return value.(*Document), nil
			}
		}
	}

	return nil, errors.New(DocumentNotFound)
}

func (index *SubIndex) Scan(hashRaw, sinceRaw interface{}, limit int) (result []Data, next interface{}, err error) {
	result = make([]Data, 0)
	hash := ParseField(index.hashKey, hashRaw)
	since := index.parseSubSortKey(sinceRaw)
	if hash == nil {
		return result, next, errors.New(HashKeyMissing)
	}

	if list := index.skipList(hash); list != nil {
		it := list.Begin(since)
		for i := 0; i < limit && it.Present(); i, _ = i+1, it.Next() {
			result = append(result, it.Value().(*Document).Data())
		}
		if it.Present() {
			next = it.Key()
		}
	}
	return result, next, nil
}

func (index *SubIndex) RScan(hashRaw, sinceRaw interface{}, limit int) (result []Data, next interface{}, err error) {
	result = make([]Data, 0)
	hash := ParseField(index.hashKey, hashRaw)
	since := index.parseSubSortKey(sinceRaw)
	if hash == nil {
		return result, next, errors.New(HashKeyMissing)
	}

	if list := index.skipList(hash); list != nil {
		it := list.End(since)
		for i := 0; i < limit && it.Present(); i, _ = i+1, it.Prev() {
			result = append(result, it.Value().(*Document).Data())
		}
		if it.Present() {
			next = it.Key()
		}
	}
	return result, next, nil
}

func (index *SubIndex) parseSubSortKey(raw interface{}) SubSortKey {
	if raw == nil {
		return SubSortKey{}
	}

	switch raw.(type) {
	case SubSortKey:
		return raw.(SubSortKey)

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

func (index *PrimaryIndex) put(doc *Document, onUpdate lazyskiplist.OnUpdate) (*Document, *Document, bool) {
	hashValue := doc.Get(index.hashKey)
	sortValue := doc.Get(index.sortKey)

	newSkipList := lazyskiplist.NewLazySkipList(GeneralCompare)
	read, _ := index.m.LoadOrStore(hashValue, newSkipList)
	list := read.(*lazyskiplist.SkipList)

	if old, newbie, replaced := list.Put(sortValue, doc, onUpdate); replaced {
		return old.(*Document), newbie.(*Document), true
	} else {
		atomic.AddInt64(&index.size, 1)
		return nil, newbie.(*Document), false
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

	if _, _, replaced := list.Put(sort, doc, nil); !replaced {
		atomic.AddInt64(&index.size, 1)
	}
}

func (index *PrimaryIndex) remove(hashRaw, sortRaw interface{}) (*Document, error) {
	hash, sort, err := index.parseKeys(hashRaw, sortRaw)
	if err != nil {
		return nil, err
	}

	if list := index.skipList(hash); list != nil {
		if value, ok := list.Remove(sort); ok {
			atomic.AddInt64(&index.size, -1)
			return value.(*Document), nil
		}
	}
	return nil, errors.New(DocumentNotFound)
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
