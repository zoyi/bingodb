package model

import (
	"fmt"
	"github.com/emirpasic/gods/utils"
	"github.com/zoyi/bingodb/ds/redblacktree"
)

type Keeper struct {
	tree *redblacktree.Tree
}

type ExpireKey struct {
	ExpiresAt int64
	Table     *Table
	*Document
}

func comparator(aRaw, bRaw interface{}) int {
	a := aRaw.(*ExpireKey)
	b := bRaw.(*ExpireKey)

	if a.Table.Bingo != b.Table.Bingo {
		panic(fmt.Sprintf("Two bingos are diffenrent: %v, %v", a.Table.Bingo, b.Table.Bingo))
	}

	var diff int

	diff = NumberComparator(a.ExpiresAt, b.ExpiresAt)
	if diff != 0 {
		return diff
	}

	diff = utils.StringComparator(a.Table.Name, b.Table.Name)
	if diff != 0 {
		return diff
	}

	if a.Table != b.Table {
		panic(fmt.Sprintf("Two tables are diffenrent: %v, %v", a.Table, b.Table))
	}

	return a.Table.Schema.PrimaryKey.Compare(a.Document, b.Document)
}

func newKeeper() *Keeper {
	return &Keeper{tree: &redblacktree.Tree{Comparator: comparator}}
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

func (keeper *Keeper) String() string {
	return keeper.tree.String()
}
