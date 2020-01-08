package shardmaster

import (
	"hash"
	"math/big"
	"strconv"
)

type CHash struct {
	vnodeNum    int
	nodeTree    *rbt
	nodeMap     map[string][]uint
	clientCache map[string]string
	hashFn      hash.Hash
}

func NewCHash(hashFn hash.Hash, vnodeNum int) *CHash {
	h := new(CHash)
	h.hashFn = hashFn
	h.nodeTree = newRBT()
	if vnodeNum < 1 {
		panic("at least 1 vnode!")
	}
	h.vnodeNum = vnodeNum
	h.nodeMap = make(map[string][]uint)
	h.clientCache = make(map[string]string)
	return h
}

func (h CHash) doHash(key string) uint {
	h.hashFn.Reset()
	buf := []byte(key)
	h.hashFn.Write(buf)
	sum := h.hashFn.Sum(nil)
	v := new(big.Int).SetBytes(sum)
	v.Mod(v, big.NewInt(1<<32))
	return uint(v.Uint64())
}

func (h *CHash) addOneNode(key string, id int) {
	hashValue := h.doHash(key + "#" + strconv.Itoa(id))
	nodes, ok := h.nodeMap[key]
	if !ok || nodes == nil {
		h.nodeMap[key] = make([]uint, h.vnodeNum)
		nodes = h.nodeMap[key]
	}
	nodes[id] = hashValue
	h.nodeTree.Insert(newRBTNode(hashValue, key))
}

func (h *CHash) removeOneNode(key string, id int) {
	h.nodeTree.Delete(h.nodeMap[key][id])
}

func (h *CHash) AddNode(key string) *CHash {
	if _, ok := h.nodeMap[key]; ok {
		panic(key + " exists, can not be add!")
	}
	for i := 0; i < h.vnodeNum; i++ {
		h.addOneNode(key, i)
	}
	h.clientCache = make(map[string]string)
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
	h.clientCache = make(map[string]string)
	return h
}

func (h CHash) GetNode(key string) (result string) {
	if v, ok := h.clientCache[key]; ok {
		result = v
		return
	}
	hashValue := h.doHash(key)
	defer func() {
		h.clientCache[key] = result
	}()
	if s := h.nodeTree.Search(hashValue); !h.nodeTree.IsNil(s) {
		result = s.payload.(string)
		return
	}
	n := newRBTNode(hashValue, key)
	h.nodeTree.Insert(n)
	defer  h.nodeTree.Delete(hashValue)
	s := h.nodeTree.Successor(n, nil)
	if h.nodeTree.IsNil(s) {
		result = h.nodeTree.Min(nil).payload.(string)
		return
	}
	result = s.payload.(string)
	return
}
