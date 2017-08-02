package model

import (
	"fmt"
	"github.com/emirpasic/gods/utils"
	"github.com/zoyi/bingodb/ds/redblacktree"
	"github.com/zoyi/bingodb/util"
)

type Keeper struct {
	tree *redblacktree.Tree
}

type ExpireKey struct {
	ExpiresAt int64
	Table     *Table
	*Document
}

func Comparator(aRaw, bRaw interface{}) int {
	a := aRaw.(*ExpireKey)
	b := bRaw.(*ExpireKey)

	var diff int

	diff = NumberComparator(a.ExpiresAt, b.ExpiresAt)
	if diff != 0 {
		return diff
	}

	if a.Table == nil || b.Table == nil {
		if a.Table == b.Table {
			return 0
		} else if a.Table == nil {
			return -1
		} else {
			return 1
		}
	}

	diff = utils.StringComparator(a.Table.Name, b.Table.Name)
	if diff != 0 {
		return diff
	}

	if a.Table != b.Table {
		panic(fmt.Sprintf("Two tables are different: %v, %v", a.Table, b.Table))
	}

	return a.Table.Schema.PrimaryKey.Compare(a.Document, b.Document)
}

func newKeeper() *Keeper {
	return &Keeper{tree: &redblacktree.Tree{Comparator: Comparator}}
}

func (keeper *Keeper) put(table *Table, doc *Document) {
	value, ok := doc.GetExpiresAt()
	if ok {
		key := &ExpireKey{ExpiresAt: value, Table: table, Document: doc}
		keeper.tree.Put(key, nil)
	}
}

func (keeper *Keeper) delete(table *Table, doc *Document) {
	value, ok := doc.GetExpiresAt()
	if ok {
		key := &ExpireKey{ExpiresAt: value, Table: table, Document: doc}
		keeper.tree.Remove(key)
	}
}

func (keeper *Keeper) Expire() {
	it := keeper.tree.RFind(&ExpireKey{ExpiresAt: util.Now().Millisecond()})

	for it.Present() {
		expireKey := it.Key().(*ExpireKey)
		fmt.Println(keeper.tree.String())
		fmt.Println(expireKey.Document)
		expireKey.Table.Delete(expireKey.Document.PrimaryKeyValue())
	}
}

func (keeper *Keeper) String() string {
	return keeper.tree.String()
}
