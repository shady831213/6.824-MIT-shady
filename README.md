# 6.824-MIT
MIT 6.824-2018 Labs

## [Course Video](https://www.bilibili.com/video/av38073607/?p=2)

## [Course Home](https://pdos.csail.mit.edu/6.824/index.html)

## Origin Repo
git://g.csail.mit.edu/6.824-golabs-2018

if the origin repo can't be access, use following command can get it:
```
git checkout 8573c99cec9a0993c025ad385e6fbc9c7526a394
```

## others
There is a ConsistentHash and RBTree implementation with tests in shardmaster. RBTree is refer to https://github.com/shady831213/algorithms/tree/master/tree/binaryTree

test:
```
cd shardmaster
go test -v -run CHash
```

# Collaboration Policy
You must write all the code you hand in for 6.824, except for code that we give you as part of the assignment. You are not allowed to look at anyone else's solution, you are not allowed to look at code from previous years, and you are not allowed to look at other Raft implementations. You may discuss the assignments with other students, but you may not look at or copy each others' code.
