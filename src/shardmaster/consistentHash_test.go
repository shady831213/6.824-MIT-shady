package shardmaster

import (
	"crypto/md5"
	"strconv"
	"testing"
)

func TestCHashAddAndRemove(t *testing.T) {
	chash := NewCHash(md5.New(), 3)
	chash.AddNode(strconv.Itoa(8))
	chash.AddNode(strconv.Itoa(1))
	chash.AddNode(strconv.Itoa(4))
	allNodes := make([]uint, 0)
	chash.nodeTree.PreOrderWalk(nil, func(i *rbt, node *rbTreeNode) bool {
		allNodes = append(allNodes, node.key)
		return false
	})
	if len(allNodes) != 3*chash.vnodeNum {
		t.Error("expect insert", 3*chash.vnodeNum, "nodes but insert", len(allNodes))
	}
	if r := chash.GetNode("8#1"); r != "8" {
		t.Error("expect 8 but get", r)
	}
	chash.RemoveNode(strconv.Itoa(1))
	chash.RemoveNode(strconv.Itoa(8))
	allNodes = make([]uint, 0)
	chash.nodeTree.PreOrderWalk(nil, func(i *rbt, node *rbTreeNode) bool {
		allNodes = append(allNodes, node.key)
		return false
	})
	if len(allNodes) != chash.vnodeNum {
		t.Error("expect remain", chash.vnodeNum, "nodes but remain", len(allNodes))
	}
}

func checkCHashBalance(t *testing.T, chash *CHash, exp map[string][]string) map[string][]string {
	nshards := 10
	groups := make(map[string][]string)
	for i := 0; i < nshards; i++ {
		g := chash.GetNode(strconv.Itoa(i))
		if _, ok := groups[g]; !ok {
			groups[g] = make([]string, 0)
		}
		groups[g] = append(groups[g], strconv.Itoa(i))
	}
	for k, v := range exp {
		for i, vv := range v {
			if groups[k][i] != vv {
				t.Error("expect key", k, "@", i, "=", vv, "but get", groups[k][i])
			}
		}
	}
	return groups
}

func TestCHashBalance(t *testing.T) {
	chash := NewCHash(md5.New(), 6)
	chash.AddNode(strconv.Itoa(8))
	chash.AddNode(strconv.Itoa(1))
	chash.AddNode(strconv.Itoa(4))
	checkCHashBalance(t, chash, map[string][]string{
		"8": {"1", "3", "7"},
		"4": {"2", "6", "8", "9"},
		"1": {"0", "4", "5"},
	})
	//add new node
	chash.AddNode(strconv.Itoa(9))
	checkCHashBalance(t, chash, map[string][]string{
		"8": {"1", "3"},
		"4": {"2", "6", "9"},
		"1": {"0", "4", "5"},
		"9": {"7", "8"},
	})
	//delete 2 nodes
	chash.RemoveNode(strconv.Itoa(4))
	checkCHashBalance(t, chash, map[string][]string{
		"8": {"1", "2", "3", "6", "9"},
		"1": {"0", "4", "5"},
		"9": {"7", "8"},})
	chash.RemoveNode(strconv.Itoa(1))
	checkCHashBalance(t, chash, map[string][]string{
		"8": {"1", "2", "3", "4", "5", "6", "9"},
		"9": {"0", "7", "8"},
	})
}
