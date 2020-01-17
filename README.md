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

## Status
- [x] Lab 1
- [x] Lab 2
- [x] Lab 3
- [x] Lab 4
  - [x] Challenge 1
  - [x] Challenge 2

## Performance
 env is a 8 G 4 cores Unbuntu16.04 Virtual Machine.
 - Lab1
 
 ![](https://github.com/shady831213/6.824-MIT-shady/blob/master/resources/Lab1.PNG)
 - Lab 2
  
 ![](https://github.com/shady831213/6.824-MIT-shady/blob/master/resources/Lab21.PNG)
 ![](https://github.com/shady831213/6.824-MIT-shady/blob/master/resources/Lab22.PNG)
 - Lab 3
   
 ![](https://github.com/shady831213/6.824-MIT-shady/blob/master/resources/Lab31.PNG)
 ![](https://github.com/shady831213/6.824-MIT-shady/blob/master/resources/Lab32.PNG)
 - Lab 4 with 2 challenges
     
 ![](https://github.com/shady831213/6.824-MIT-shady/blob/master/resources/Lab4.PNG)
  
## Lab4Notes
There is a ConsistentHash and RBTree implementation with tests in shardmaster. RBTree is refer to https://github.com/shady831213/algorithms/tree/master/tree/binaryTree

test:
```
cd shardmaster
go test -v -run CHash
```
Because hash function can not meet strict balance, balance check in lab test is relaxed.


# Collaboration Policy
You must write all the code you hand in for 6.824, except for code that we give you as part of the assignment. You are not allowed to look at anyone else's solution, you are not allowed to look at code from previous years, and you are not allowed to look at other Raft implementations. You may discuss the assignments with other students, but you may not look at or copy each others' code.
