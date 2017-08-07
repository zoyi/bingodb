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
		}
		return a.Table == nil
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
	currentTime := util.Now().Millisecond()

	for true {
		if key, ok := keeper.list.SeekToFirst().Key().(*ExpireKey); ok {
			if key.ExpiresAt < currentTime {
				key.Table.Delete(key.Document.PrimaryKeyValue())
			}
		} else {
			break
		}
	}
}

func (keeper *Keeper) String() string {
	return keeper.list.String()
}
