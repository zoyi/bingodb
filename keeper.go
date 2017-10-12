package bingodb

import (
	"fmt"
	"github.com/emirpasic/gods/utils"
	"github.com/robfig/cron"
	"github.com/zoyi/skiplist/lazy"
	"time"
)

type Keeper struct {
	bingo *Bingo
	list  *lazyskiplist.SkipList
	cron  *cron.Cron
}

type ExpireKey struct {
	expiresAt int64
	table     *Table
	*Document
}

func comparator(aRaw, bRaw interface{}) int {
	a := aRaw.(*ExpireKey)
	b := bRaw.(*ExpireKey)

	var diff int

	diff = NumberComparator(a.expiresAt, b.expiresAt)
	if diff != 0 {
		return diff
	}

	if a.table == nil || b.table == nil {
		if a.table == b.table {
			return 0
		} else if a.table == nil {
			return -1
		} else {
			return 1
		}
	}

	diff = utils.StringComparator(a.table.name, b.table.name)
	if diff != 0 {
		return diff
	}

	if a.table != b.table {
		panic(fmt.Sprintf("Two Tables are different: %v, %v", a.table, b.table))
	}

	return a.table.Compare(a.Document, b.Document)
}

func NewKeeper(bingo *Bingo) *Keeper {
	keeper := &Keeper{bingo: bingo, list: lazyskiplist.NewLazySkipList(comparator)}
	c := cron.New()
	c.AddFunc("@every 1s", keeper.expire)
	keeper.cron = c
	return keeper
}

func (keeper *Keeper) put(table *Table, doc *Document) {
	value, ok := doc.GetExpiresAt()
	if ok {
		key := &ExpireKey{expiresAt: value, table: table, Document: doc}
		keeper.list.Put(key, nil, nil)
	}
}

func (keeper *Keeper) remove(table *Table, doc *Document) {
	value, ok := doc.GetExpiresAt()
	if ok {
		key := &ExpireKey{expiresAt: value, table: table, Document: doc}
		keeper.list.Remove(key)
	}
}

func (keeper *Keeper) expire() {
	i := 0
	for it := keeper.list.Begin(nil); it.Present(); it.Next() {
		key := it.Key().(*ExpireKey)
		if key.expiresAt > time.Now().Unix()*1000 { // For millis
			break
		}
		key.table.RemoveByDocument(key.Document)
		i++
	}
	keeper.bingo.AddExpire(int64(i))
}

func (keeper *Keeper) start() {
	keeper.cron.Start()
}

func (keeper *Keeper) stop() {
	keeper.cron.Stop()
}
