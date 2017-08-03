package redblacktree

type BaseIterator interface {
	Present() bool
	Remove() (value interface{}, removed bool)
	Next()
	Key() interface{}
	Value() interface{}
}

type Iterator struct {
	BaseIterator
	tree *Tree
	node *Node
}

type ReverseIterator struct {
	*Iterator
}

func newIterator(tree *Tree, node *Node) *Iterator {
	return &Iterator{tree: tree, node: node}
}

func newReverseIterator(tree *Tree, node *Node) *ReverseIterator {
	return &ReverseIterator{Iterator: &Iterator{tree: tree, node: node}}
}

func (tree *Tree) Begin() *Iterator {
	return newIterator(tree, tree.Left())
}

func (tree *Tree) RBegin() *ReverseIterator {
	return newReverseIterator(tree, tree.Right())
}

func (tree *Tree) Find(key interface{}) *Iterator {
	node, _ := tree.Ceiling(key)
	return newIterator(tree, node)
}

func (tree *Tree) RFind(key interface{}) *ReverseIterator {
	node, _ := tree.Floor(key)
	return newReverseIterator(tree, node)
}

func (it *Iterator) Key() interface{} {
	return it.node.Key
}

func (it *Iterator) Value() interface{} {
	return it.node.Value
}

func (it *Iterator) Present() bool {
	return it.node != nil && it.node.Key != nil
}

func (it *Iterator) Next() {
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

func (it *Iterator) Prev() {
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

	//fmt.Println("empty tree!!!")
	it.node = nil
}

func (it *ReverseIterator) Next() {
	it.Iterator.Prev()
}

func (it *ReverseIterator) Prev() {
	it.Iterator.Next()
}
