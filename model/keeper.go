package model

import (
	"fmt"
	"github.com/emirpasic/gods/utils"
	"github.com/zoyi/bingodb/ds/skiplist"
	"github.com/zoyi/bingodb/util"
)

type Keeper struct {
	list *skiplist.SkipList
}

type ExpireKey struct {
	ExpiresAt int64
	Table     *Table
	*Document
}

func comparator(l, r interface{}) bool {
	a := l.(*ExpireKey)
	b := r.(*ExpireKey)

	var diff int

	diff = NumberComparator(a.ExpiresAt, b.ExpiresAt)
	if diff != 0 {
		return diff < 0
	}

	if a.Table == nil || b.Table == nil {
		if a.Table == b.Table {
			return false
		} else if a.Table == nil {
			return false
		} else {
			return true
		}
	}

	diff = utils.StringComparator(a.Table.Name, b.Table.Name)
	if diff != 0 {
		return diff < 0
	}

	if a.Table != b.Table {
		panic(fmt.Sprintf("Two tables are different: %v, %v", a.Table, b.Table))
	}

	return a.Table.Schema.PrimaryKey.Compare(a.Document, b.Document) < 0
}

func newKeeper() *Keeper {
	return &Keeper{list: skiplist.NewCustomMap(comparator)}
}

func (keeper *Keeper) put(table *Table, doc *Document) {
	value, ok := doc.GetExpiresAt()
	if ok {
		key := &ExpireKey{ExpiresAt: value, Table: table, Document: doc}
		keeper.list.Set(key, nil)
	}
}

func (keeper *Keeper) delete(table *Table, doc *Document) {
	value, ok := doc.GetExpiresAt()
	if ok {
		key := &ExpireKey{ExpiresAt: value, Table: table, Document: doc}
		keeper.list.Delete(key)
	}
}

func (keeper *Keeper) Expire() {
	boundIterator := keeper.list.Seek(&ExpireKey{ExpiresAt: util.Now().Millisecond()})
	for boundIterator.Previous() {
		expireKey := boundIterator.Key().(*ExpireKey)
		expireKey.Table.Delete(expireKey.Document.PrimaryKeyValue())
	}
}

func (keeper *Keeper) String() string {
	return keeper.list.String()
}
