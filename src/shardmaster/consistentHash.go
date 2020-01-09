package shardmaster

import (
	"hash"
	"strconv"
)

type CHash struct {
	vnodeNum int
	nodeTree *rbt
	nodeMap  map[string][]uint
	hashFn   hash.Hash
}

func NewCHash(hashFn hash.Hash, vnodeNum int) *CHash {
	h := new(CHash)
	h.Init(hashFn, vnodeNum)
	return h
}

func (h *CHash) Init(hashFn hash.Hash, vnodeNum int) *CHash {
	h.hashFn = hashFn
	h.nodeTree = newRBT()
	if vnodeNum < 1 {
		panic("at least 1 vnode!")
	}
	h.vnodeNum = vnodeNum
	h.nodeMap = make(map[string][]uint)
	return h
}

func (h CHash) doHash(key string) uint {
	h.hashFn.Reset()
	buf := []byte(key)
	h.hashFn.Write(buf)
	for i:=0;i<10;i++ {
		s := h.hashFn.Sum(nil)
		h.hashFn.Reset()
		h.hashFn.Write(s)
	}
	sum := h.hashFn.Sum(nil)
	return uint(sum[3])<<24 | uint(sum[2])<<16 | uint(sum[1])<<8 | uint(sum[0])
}

func (h *CHash) addOneNode(key string, id int) {
	subKey := key + "#" + strconv.Itoa(id)
	hashValue := h.doHash(subKey)
	for s := h.nodeTree.Search(hashValue); !h.nodeTree.IsNil(s); s = h.nodeTree.Search(hashValue){
		subKey = subKey + "#" + strconv.Itoa(id)
		hashValue = h.doHash(subKey)
	}
	h.nodeMap[key][id] = hashValue
	h.nodeTree.Insert(newRBTNode(hashValue, key))
}

func (h *CHash) removeOneNode(key string, id int) {
	h.nodeTree.Delete(h.nodeMap[key][id])
}

func (h *CHash) AddNode(key string, weight int) *CHash {
	if weight < 1 {
		panic("weight require at least 1!")
	}

	if _, ok := h.nodeMap[key]; ok {
		return h
	}
	h.nodeMap[key] = make([]uint, weight*h.vnodeNum)
	for i := 0; i < weight*h.vnodeNum; i++ {
		h.addOneNode(key, i)
	}
	return h
}

func (h *CHash) RemoveNode(key string) *CHash {
	if _, ok := h.nodeMap[key]; !ok {
		panic(key + " does not exist, can not be remove!")
	}
	for i := range h.nodeMap[key] {
		h.removeOneNode(key, i)
	}
	delete(h.nodeMap, key)
	return h
}

func (h CHash) GetNode(key string) (result string) {
	hashValue := h.doHash(key)
	for s := h.nodeTree.Search(hashValue); !h.nodeTree.IsNil(s); {
		result = s.payload.(string)
		return
	}
	n := newRBTNode(hashValue, key)
	h.nodeTree.Insert(n)
	defer h.nodeTree.Delete(hashValue)
	s := h.nodeTree.Successor(n, nil)
	if h.nodeTree.IsNil(s) {
		result = h.nodeTree.Min(nil).payload.(string)
		return
	}
	result = s.payload.(string)
	return
}
