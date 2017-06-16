package model

import (
	"fmt"
	"bingodb/ds/redblacktree"
)


type Index struct {
	Data    map[interface{}]*redblacktree.Tree
	*Key
}

type PrimaryIndex struct {
	*Index
}

type SubIndex struct {
	*Index
}


func (index *PrimaryIndex) delete(hash interface{}, sort interface{}) (*Document, bool) {
	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	tree, present := index.Data[hash]
	if !present {
		return nil, false
	}

	value, present := tree.Remove(sort)
	if !present {
		return nil, false
	}

	return value.(*Document), true
}

func (index *SubIndex) delete(doc *Document) {
	hash := doc.Get(index.HashKey.Name)
	sort := SubSortTreeKey{Key: doc.Get(index.SortKey.Name), Document: doc}

	tree, present := index.Data[hash]
	if !present {
		return
	}

	tree.Remove(sort)
}

func (index *PrimaryIndex) Get(hash interface{}, sort interface{}) (*Document, bool) {
	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	tree, present := index.Data[hash]
	if !present {
		return nil, false
	}

	value, present := tree.Get(sort)
	if !present {
		return nil, false
	}

	return value.(*Document), true
}

func (index *SubIndex) Get(hash interface{}, sort interface{}) (*Document, bool) {
	hash = index.HashKey.Parse(hash)
	sort = index.SortKey.Parse(sort)

	tree, present := index.Data[hash]
	if !present {
		return nil, false
	}

	fmt.Print(tree.String())

	node, present := tree.Ceiling(SubSortTreeKey{Key: sort})
	if !present {
		return nil, false
	}

	return node.Key.(SubSortTreeKey).Document, true
}

func (index *PrimaryIndex) put(doc *Document) (replaced bool, removed interface{}) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := doc.Get(index.SortKey.Name)

	tree, present := index.Data[hashValue]
	if !present {
		tree = redblacktree.NewWithStringComparator()
		// &redblacktree.Tree{Comparator: Compare}
		index.Data[hashValue] = tree
	}

	return tree.Put(sortValue, doc)
}

func (index *SubIndex) put(doc *Document) {
	hashValue := doc.Get(index.HashKey.Name)
	sortValue := SubSortTreeKey{Key: doc.Get(index.SortKey.Name), Document: doc}

	tree, present := index.Data[hashValue]
	if !present {
		tree = &redblacktree.Tree{Comparator: func(a, b interface{}) int {
			ka := a.(SubSortTreeKey)
			kb := b.(SubSortTreeKey)
			o := GeneralCompare(ka.Key, kb.Key)
			if o != 0 {
				return o
			}

			//fmt.Println("here!!!!!!!!!!!")
			//fmt.Println(ka)
			//fmt.Println(ka.Document)
			//fmt.Println(kb)
			//fmt.Println(kb.Document)
			//fmt.Println("res: ", ka.schema.PrimaryKey.Compare(ka.Document, kb.Document))
			return doc.schema.PrimaryKey.Compare(ka.Document, kb.Document)
		},
		}
		index.Data[hashValue] = tree
	}

	tree.Put(sortValue, nil)
}
