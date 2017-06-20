package redblacktree


type Iterator interface {
	Present() bool
	Remove() (value interface{}, removed bool)
	Next()
	Key() (interface{})
	Value() (interface{})
}

type ForwardIterator struct {
	tree     *Tree
	node     *Node
}

type BackwardIterator struct {
	*ForwardIterator
}

type Director struct {
	tree     *Tree
	direction Direction
}

type Direction byte

const (
	Forward  Direction = 0
	Backward Direction = 1
)

func (tree *Tree) Forward() Director {
	return Director{tree: tree, direction: Forward}
}

func (tree *Tree) Backward() Director {
	return Director{tree: tree, direction: Backward}
}

func (director Director) Begin() Iterator {
	tree := director.tree

	node := tree.Root

	if tree.Root != nil {
		if director.direction == Forward {
			node = tree.Left()
		} else {
			node = tree.Right()
		}
	}

	if director.direction == Forward {
		return &ForwardIterator{tree: tree, node: node}
	} else {
		return &BackwardIterator{ForwardIterator: &ForwardIterator{tree: tree, node: node}}
	}
}

func (it *ForwardIterator) Key() interface{} {
	return it.node.Key
}

func (it *ForwardIterator) Value() interface{} {
	return it.node.Value
}

func (it *ForwardIterator) Present() bool {
	return it.node != nil
}

func (it *ForwardIterator) Remove() (value interface{}, removed bool) {
	return nil, false
}

func (it *ForwardIterator) Next() {
	if it.node.Right != nil {
		it.node = it.node.Right
		for it.node.Left != nil {
			it.node = it.node.Left
		}
		return
	}

	if it.node.Parent != nil {
		node := it.node
		for it.node.Parent != nil {
			it.node = it.node.Parent
			if it.tree.Comparator(node.Key, it.node.Key) <= 0 {
				return
			}
		}
	}

	it.node = nil
}

func (it *ForwardIterator) Prev() {
	if it.node.Left != nil {
		it.node = it.node.Left
		for it.node.Right != nil {
			it.node = it.node.Right
		}
		return
	}

	if it.node.Parent != nil {
		node := it.node
		for it.node.Parent != nil {
			it.node = it.node.Parent
			if it.tree.Comparator(node.Key, it.node.Key) >= 0 {
				return
			}
		}
	}

	it.node = nil
}

func (it *BackwardIterator) Next() {
	it.ForwardIterator.Prev()
}

func (it *BackwardIterator) Prev() {
	it.ForwardIterator.Next()
}


// Copyright (c) 2015, Emir Pasic. All rights reserved.
//// Use of this source code is governed by a BSD-style
//// license that can be found in the LICENSE file.
//
//package redblacktree
//
//import "github.com/emirpasic/gods/containers"
//
//func assertIteratorImplementation() {
//	var _ containers.ReverseIteratorWithKey = (*Iterator)(nil)
//}
//
//// Iterator holding the it's state
//type Iterator struct {
//	tree     *Tree
//	node     *Node
//	position position
//}
//
//type position byte
//
//const (
//	begin, between, end position = 0, 1, 2
//)
//
//// Iterator returns a stateful it whose elements are key/value pairs.
//func (tree *Tree) Iterator() Iterator {
//	return Iterator{tree: tree, node: nil, position: begin}
//}
//
//// Next moves the it to the next element and returns true if there was a next element in the container.
//// If Next() returns true, then next element's key and value can be retrieved by Key() and Value().
//// If Next() was called for the first time, then it will point the it to the first element if it exists.
//// Modifies the state of the it.
//func (it *Iterator) Next() bool {
//	if it.position == end {
//		goto end
//	}
//	if it.position == begin {
//		left := it.tree.Left()
//		if left == nil {
//			goto end
//		}
//		it.node = left
//		goto between
//	}
//	if it.node.Right != nil {
//		it.node = it.node.Right
//		for it.node.Left != nil {
//			it.node = it.node.Left
//		}
//		goto between
//	}
//	if it.node.Parent != nil {
//		node := it.node
//		for it.node.Parent != nil {
//			it.node = it.node.Parent
//			if it.tree.Comparator(node.Key, it.node.Key) <= 0 {
//				goto between
//			}
//		}
//	}
//
//end:
//	it.node = nil
//	it.position = end
//	return false
//
//between:
//	it.position = between
//	return true
//}
//
//// Prev moves the it to the previous element and returns true if there was a previous element in the container.
//// If Prev() returns true, then previous element's key and value can be retrieved by Key() and Value().
//// Modifies the state of the it.
//func (it *Iterator) Prev() bool {
//	if it.position == begin {
//		goto begin
//	}
//	if it.position == end {
//		right := it.tree.Right()
//		if right == nil {
//			goto begin
//		}
//		it.node = right
//		goto between
//	}
//	if it.node.Left != nil {
//		it.node = it.node.Left
//		for it.node.Right != nil {
//			it.node = it.node.Right
//		}
//		goto between
//	}
//	if it.node.Parent != nil {
//		node := it.node
//		for it.node.Parent != nil {
//			it.node = it.node.Parent
//			if it.tree.Comparator(node.Key, it.node.Key) >= 0 {
//				goto between
//			}
//		}
//	}
//
//begin:
//	it.node = nil
//	it.position = begin
//	return false
//
//between:
//	it.position = between
//	return true
//}
//
//// Value returns the current element's value.
//// Does not modify the state of the it.
//func (it *Iterator) Value() interface{} {
//	return it.node.Value
//}
//
//// Key returns the current element's key.
//// Does not modify the state of the it.
//func (it *Iterator) Key() interface{} {
//	return it.node.Key
//}
//
//// Begin resets the it to its initial state (one-before-first)
//// Call Next() to fetch the first element if any.
//func (it *Iterator) Begin() {
//	it.node = nil
//	it.position = begin
//}
//
//// End moves the it past the last element (one-past-the-end).
//// Call Prev() to fetch the last element if any.
//func (it *Iterator) End() {
//	it.node = nil
//	it.position = end
//}
//
//// First moves the it to the first element and returns true if there was a first element in the container.
//// If First() returns true, then first element's key and value can be retrieved by Key() and Value().
//// Modifies the state of the it
//func (it *Iterator) First() bool {
//	it.Begin()
//	return it.Next()
//}
//
//// Last moves the it to the last element and returns true if there was a last element in the container.
//// If Last() returns true, then last element's key and value can be retrieved by Key() and Value().
//// Modifies the state of the it.
//func (it *Iterator) Last() bool {
//	it.End()
//	return it.Prev()
//}
