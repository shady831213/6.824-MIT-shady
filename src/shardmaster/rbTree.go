package shardmaster

type rbTreeColor bool

const (
	black = true
	red   = false
)

type rbTreeSide bool

const (
	left  = true
	right = false
)

type rbTreeNode struct {
	key                 uint
	Parent, Left, Right *rbTreeNode
	color               rbTreeColor
	payload             interface{}
}

func newRBTNode(key uint, payload interface{}) *rbTreeNode {
	n := new(rbTreeNode)
	n.key = key
	n.payload = payload
	return n
}

func (n *rbTreeNode) Less(r *rbTreeNode) bool {
	return n.key < r.key
}

func (n *rbTreeNode) Swap(r *rbTreeNode) {
	n.key, n.payload, r.key, r.payload = r.key, r.payload, n.key, n.payload
}

func (n *rbTreeNode) KeyLess(key uint) bool {
	return n.key < key
}

type rbt struct {
	NilNode *rbTreeNode
}

func (t *rbt) init() {
	t.NilNode = new(rbTreeNode)
	t.NilNode.Left = t.NilNode
	t.NilNode.Right = t.NilNode
	t.NilNode.Parent = t.NilNode
}

func (t *rbt) IsNil(n *rbTreeNode) bool {
	return n == t.NilNode
}

func (t *rbt) Root() *rbTreeNode {
	return t.NilNode.Left
}

func (t *rbt) setColor(node *rbTreeNode, color rbTreeColor) {
	node.color = color
}

func (t *rbt) color(node *rbTreeNode) rbTreeColor {
	return rbTreeColor(t.IsNil(node)) || node.color
}

func (t *rbt) otherSideNode(side rbTreeSide, node *rbTreeNode) *rbTreeNode {
	if side == left {
		return node.Right
	}
	return node.Left
}

func (t *rbt) LeftRotate(n *rbTreeNode) *rbTreeNode {
	if t.IsNil(n.Right) {
		return t.NilNode
	}
	newNode := n.Right
	if n.Parent.Left == n {
		n.Parent.Left = newNode
	}
	if n.Parent.Right == n {
		n.Parent.Right = newNode
	}
	n.Parent, newNode.Parent = newNode, n.Parent
	newNode.Left, n.Right = n, newNode.Left
	if !t.IsNil(n.Right) {
		n.Right.Parent = n
	}
	return newNode
}

func (t *rbt) RightRotate(n *rbTreeNode) *rbTreeNode {
	if t.IsNil(n.Left) {
		return t.NilNode
	}
	newNode := n.Left
	if n.Parent.Right == n {
		n.Parent.Right = newNode
	}
	if n.Parent.Left == n {
		n.Parent.Left = newNode
	}
	n.Parent, newNode.Parent = newNode, n.Parent
	newNode.Right, n.Left = n, newNode.Right
	if !t.IsNil(n.Left) {
		n.Left.Parent = n
	}
	return newNode
}

func (t *rbt) invDirRotation(side rbTreeSide, node *rbTreeNode) interface{} {
	if side == left {
		return t.RightRotate(node)
	}
	return t.LeftRotate(node)
}
func (t *rbt) sameSideNode(side rbTreeSide, node *rbTreeNode) *rbTreeNode {
	if side == left {
		return node.Left
	}
	return node.Right
}

func (t *rbt) sameDirRotation(side rbTreeSide, node *rbTreeNode) interface{} {
	if side == left {
		return t.LeftRotate(node)
	}
	return t.RightRotate(node)
}

func (t *rbt) Min(node *rbTreeNode) *rbTreeNode {
	cur := node
	if cur == nil {
		cur = t.Root()
	}
	for !t.IsNil(cur.Left) {
		cur = cur.Left
	}
	return cur
}

func (t *rbt) Max(node *rbTreeNode) *rbTreeNode {
	cur := node
	if cur == nil {
		cur = t.Root()
	}
	for !t.IsNil(cur.Right) {
		cur = cur.Right
	}
	return cur
}

func (t *rbt) Predecessor(node *rbTreeNode, root *rbTreeNode) *rbTreeNode {
	r := root
	if r == nil {
		r = t.Root()
	}
	n := node
	if t.IsNil(n) {
		return t.NilNode
	}
	if !t.IsNil(n.Left) {
		return t.Max(n.Left)
	}
	cur := n
	for cur != r && cur.Parent.Right != cur {
		cur = cur.Parent
	}
	if cur == r {
		return t.NilNode
	}
	return cur.Parent
}

func (t *rbt) Successor(node *rbTreeNode, root *rbTreeNode) *rbTreeNode {
	r := root
	if r == nil {
		r = t.Root()
	}
	n := node
	if t.IsNil(n) {
		return t.NilNode
	}
	if !t.IsNil(n.Right) {
		return t.Min(n.Right)
	}
	cur := n
	for cur != r && cur.Parent.Left != cur {
		cur = cur.Parent
	}
	if cur == r {
		return t.NilNode
	}
	return cur.Parent
}

func (t *rbt) insert(n *rbTreeNode) *rbTreeNode {
	target := t.Root()
	n.Left = t.NilNode
	n.Right = t.NilNode
	n.Parent = t.NilNode
	for cur := t.Root(); !t.IsNil(cur); {
		target = cur
		if n.Less(cur) {
			cur = cur.Left
		} else {
			cur = cur.Right
		}
	}
	n.Parent = target
	if t.IsNil(target) {
		t.NilNode.Left = n
		t.NilNode.Right = n
	} else if n.Less(target) {
		target.Left = n
	} else {
		target.Right = n
	}

	return n
}

func (t *rbt) Insert(node *rbTreeNode) *rbTreeNode {
	n := t.insert(node)
	t.setColor(n, red)
	t.insertFix(n)
	return n
}

func (t *rbt) insertFix(n *rbTreeNode) {
	//only can violate property 3: both left and right children of red node must be black
	for !t.color(n.Parent) && !t.color(n) {
		grandNode := n.Parent.Parent //must be black
		uncleNode := grandNode.Right
		if n.Parent == uncleNode {
			uncleNode = grandNode.Left
		}
		//case1: uncle node is red
		if !t.color(uncleNode) {
			t.setColor(grandNode, red)
			t.setColor(grandNode.Left, black)
			t.setColor(grandNode.Right, black)
			n = grandNode
			//case2&3: uncle node is black
		} else {
			side := rbTreeSide(n.Parent == grandNode.Left)
			t.setColor(grandNode, red)
			//case 2 n is right child of parent
			if n == t.otherSideNode(side, n.Parent) {
				t.sameDirRotation(side, n.Parent)
			}
			//case 3 n is left child of parent
			t.setColor(t.sameSideNode(side, grandNode), black)
			t.invDirRotation(side, grandNode)
		}
	}
	t.setColor(t.Root(), black)
}

func (t *rbt) Search(key uint) *rbTreeNode {
	for cur := t.Root(); !t.IsNil(cur); {
		if cur.key == key {
			return cur
		} else if cur.KeyLess(key) {
			cur = cur.Right
		} else {
			cur = cur.Left
		}
	}
	return t.NilNode
}

func (t *rbt) Delete(key uint) *rbTreeNode {
	deleteNonCompletedNode := func(node *rbTreeNode) (deletedNode *rbTreeNode, nextNode *rbTreeNode) {
		var reConnectedNode *rbTreeNode
		if t.IsNil(node.Left) {
			reConnectedNode = node.Right
		} else {
			reConnectedNode = node.Left
		}
		//mean's another black color
		reConnectedNode.Parent = node.Parent
		if t.IsNil(node.Parent) {
			t.NilNode.Left = reConnectedNode
			t.NilNode.Right = reConnectedNode
		} else if node.Parent.Right == node {
			node.Parent.Right = reConnectedNode
		} else {
			node.Parent.Left = reConnectedNode
		}
		return node, reConnectedNode
	}
	node := t.Search(key)
	if t.IsNil(node) {
		return node
	}
	var deletedNode, reConnectedNode *rbTreeNode
	if t.IsNil(node.Left) || t.IsNil(node.Right) {
		deletedNode, reConnectedNode = deleteNonCompletedNode(node)
	} else {
		successor := t.Successor(node, t.Root())
		node.Swap(successor)
		deletedNode, reConnectedNode = deleteNonCompletedNode(successor)
	}
	if t.color(deletedNode) {
		//Now, reConnectedNode is black-black or black-red
		t.deleteFix(reConnectedNode)
	}
	//recover NilNode
	t.NilNode.Parent = t.NilNode
	return deletedNode
}

func (t *rbt) deleteFix(n *rbTreeNode) {
	//n always points to the black-black or black-red node.The purpose is to remove the additional black color,
	//which means add a black color in the same side or reduce a black color in the other side
	for n != t.Root() && t.color(n) {
		side := rbTreeSide(n == n.Parent.Left)
		brotherNode := t.otherSideNode(side, n.Parent)
		//case 1 brotherNode node is red, so parent must be black.Turn brotherNode node to a black one, convert to case 2,3,4
		if !t.color(brotherNode) {
			t.setColor(n.Parent, red)
			t.setColor(brotherNode, black)
			t.sameDirRotation(side, n.Parent)
			//case 2, 3, 4 brotherNode node is black
		} else {
			//case 2 move black-blcak or blcak-red node up
			if t.color(brotherNode.Left) && t.color(brotherNode.Right) {
				t.setColor(brotherNode, red)
				n = n.Parent
				//case 3 convert to case 4
			} else if t.color(t.otherSideNode(side, brotherNode)) {
				t.setColor(brotherNode, red)
				t.setColor(t.sameSideNode(side, brotherNode), black)
				t.invDirRotation(side, brotherNode)
				//case 4 add a black to left, turn black-black or black-red to black or red
			} else {
				t.setColor(brotherNode, t.color(n.Parent))
				t.setColor(n.Parent, black)
				t.setColor(t.otherSideNode(side, brotherNode), black)
				t.sameDirRotation(side, n.Parent)
				n = t.Root()
			}
		}

	}
	t.setColor(n, black)

}

func (t *rbt) InOrderWalk(node *rbTreeNode, callback func(*rbt, *rbTreeNode) bool) bool {
	n := node
	if n == nil {
		n = t.Root()
	}
	if !t.IsNil(n) {
		stop := t.InOrderWalk(n.Left, callback)
		if stop {
			return true
		}
		stop = callback(t, n)
		if stop {
			return true
		}
		stop = t.InOrderWalk(n.Right, callback)
		return stop
	}
	return false
}

func (t *rbt) PreOrderWalk(node *rbTreeNode, callback func(*rbt, *rbTreeNode) bool) bool {
	n := node
	if n == nil {
		n = t.Root()
	}
	if !t.IsNil(n) {
		stop := callback(t, n)
		if stop {
			return true
		}
		stop = t.PreOrderWalk(n.Left, callback)
		if stop {
			return true
		}
		stop = t.PreOrderWalk(n.Right, callback)
		return stop
	}
	return false
}

func (t *rbt) PostOrderWalk(node *rbTreeNode, callback func(*rbt, *rbTreeNode) bool) bool {
	n := node
	if n == nil {
		n = t.Root()
	}
	if !t.IsNil(n) {
		stop := t.PostOrderWalk(n.Left, callback)
		if stop {
			return true
		}
		stop = t.PostOrderWalk(n.Right, callback)
		if stop {
			return true
		}
		stop = callback(t, n)
		return stop
	}
	return false
}

func newRBT() *rbt {
	t := new(rbt)
	t.init()
	return t
}
