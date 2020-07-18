package btree

import (
	"fmt"
	"sync"
)

func search(parent *node, key Key) Key {
	parent = getParent(parent, nil, key)
	parent.lock.RLock()
	parent = moveRight(parent, key, false)
	defer parent.lock.RUnlock()

	i := parent.search(key)
	if i == len(parent.Keys) {
		return Key{isNull: true}
	}

	return parent.Keys[i]
}

func getParent(parent *node, stack *nodes, key Key) *node {
	var n *node
	for parent != nil && !parent.IsLeaf {
		parent.lock.RLock()
		parent = moveRight(parent, key, false) // if this happens on the Root this should always just return
		n = parent.searchNode(key)
		if stack != nil {
			stack.push(parent)
		}

		parent.lock.RUnlock()
		parent = n
	}

	return parent
}

func insert(tree *Blink, parent *node, stack *nodes, key Key) Key {
	parent = getParent(parent, stack, key)

	parent.lock.Lock()
	parent = moveRight(parent, key, true)

	result := parent.insert(key)
	if result.isNull != true { // overwrite
		parent.lock.Unlock()
		return result
	}

	if !parent.needsSplit() {
		parent.lock.Unlock()
		return Key{isNull: true}
	}

	split(tree, parent, stack)

	return Key{isNull: true}
}

func split(tree *Blink, n *node, stack *nodes) {
	var l, r *node
	var k Key
	var parent *node
	for n.needsSplit() {
		k, l, r = n.split()
		parent = stack.pop()
		if parent == nil {
			tree.lock.Lock()
			if tree.Root == nil || tree.Root == n {
				parent = newNode(false, make(Keys, 0, tree.Ary), make(nodes, 0, tree.Ary+1))
				parent.MaxSeen = r.max()
				parent.Keys.insert(k)
				parent.Nodes.push(l)
				parent.Nodes.push(r)
				tree.Root = parent
				n.lock.Unlock()
				tree.lock.Unlock()
				return
			}

			parent = tree.Root
			tree.lock.Unlock()
		}

		parent.lock.Lock()
		parent = moveRight(parent, r.key(), true)
		i := parent.search(k)
		parent.Keys.insertAt(k, i)
		parent.Nodes[i] = l
		parent.Nodes.insertAt(r, i+1)

		n.lock.Unlock()
		n = parent
	}

	n.lock.Unlock()
}

func moveRight(n *node, key Key, getLock bool) *node {
	var right *node
	for {
		if len(n.Keys) == 0 || n.Right == nil { // this is either the node or the rightmost node
			return n
		}
		if key.Compare(n.max()) < 1 {
			return n
		}

		if getLock {
			n.Right.lock.Lock()
			right = n.Right
			n.lock.Unlock()
		} else {
			n.Right.lock.RLock()
			right = n.Right
			n.lock.RUnlock()
		}
		n = right
	}
}

type nodes []*node

func (ns *nodes) reset() {
	for i := range *ns {
		(*ns)[i] = nil
	}

	*ns = (*ns)[:0]
}

func (ns *nodes) push(n *node) {
	*ns = append(*ns, n)
}

func (ns *nodes) pop() *node {
	if len(*ns) == 0 {
		return nil
	}

	n := (*ns)[len(*ns)-1]
	(*ns)[len(*ns)-1] = nil
	*ns = (*ns)[:len(*ns)-1]
	return n
}

func (ns *nodes) insertAt(n *node, i int) {
	if i == len(*ns) {
		*ns = append(*ns, n)
		return
	}

	*ns = append(*ns, nil)
	copy((*ns)[i+1:], (*ns)[i:])
	(*ns)[i] = n
}

func (ns *nodes) splitAt(i int) (nodes, nodes) {
	length := len(*ns) - i
	right := make(nodes, length, cap(*ns))
	copy(right, (*ns)[i+1:])
	for j := i + 1; j < len(*ns); j++ {
		(*ns)[j] = nil
	}
	*ns = (*ns)[:i+1]
	return *ns, right
}

type node struct {
	Keys    Keys
	Nodes   nodes
	Right   *node
	lock    sync.RWMutex
	IsLeaf  bool
	MaxSeen Key
}

func (n *node) key() Key {
	return n.Keys.last()
}

func (n *node) insert(key Key) Key {
	if !n.IsLeaf {
		panic(`Can't only insert key in an pkg node.`)
	}

	overwritten := n.Keys.insert(key)
	return overwritten
}

func (n *node) insertNode(other *node) {
	key := other.key()
	i := n.Keys.search(key)
	n.Keys.insertAt(key, i)
	n.Nodes.insertAt(other, i)
}

func (n *node) needsSplit() bool {
	return n.Keys.needsSplit()
}

func (n *node) max() Key {
	if n.IsLeaf {
		return n.Keys.last()
	}

	return n.MaxSeen
}

func (n *node) splitLeaf() (Key, *node, *node) {
	i := (len(n.Keys) / 2)
	key := n.Keys[i]
	_, rightKeys := n.Keys.splitAt(i)
	nn := &node{
		Keys:   rightKeys,
		Right:  n.Right,
		IsLeaf: true,
	}
	n.Right = nn
	return key, n, nn
}

func (n *node) splitInternal() (Key, *node, *node) {
	i := (len(n.Keys) / 2)
	key := n.Keys[i]

	rightKeys := make(Keys, len(n.Keys)-1-i, cap(n.Keys))
	rightNodes := make(nodes, len(rightKeys)+1, cap(n.Nodes))

	copy(rightKeys, n.Keys[i+1:])
	copy(rightNodes, n.Nodes[i+1:])

	// for garbage collection
	for j := i + 1; j < len(n.Nodes); j++ {
		if j != len(n.Keys) {
			n.Keys[j] = Key{isNull: true}
		}
		n.Nodes[j] = nil
	}

	nn := newNode(false, rightKeys, rightNodes)
	nn.MaxSeen = n.max()

	n.MaxSeen = key
	n.Keys = n.Keys[:i]
	n.Nodes = n.Nodes[:i+1]
	n.Right = nn

	return key, n, nn
}

func (n *node) split() (Key, *node, *node) {
	if n.IsLeaf {
		return n.splitLeaf()
	}

	return n.splitInternal()
}

func (n *node) search(key Key) int {
	return n.Keys.search(key)
}

func (n *node) searchNode(key Key) *node {
	i := n.search(key)

	return n.Nodes[i]
}

func (n *node) print() {
	fmt.Printf(`NODE: %+v, %p`, n, n)
	if !n.IsLeaf {
		for _, n := range n.Nodes {
			if n == nil {
				fmt.Println(`NIL NODE`)
				continue
			}
			n.print()
		}
	}
}

func newNode(isLeaf bool, keys Keys, ns nodes) *node {
	return &node{
		IsLeaf: isLeaf,
		Keys:   keys,
		Nodes:  ns,
	}
}