# H2研究
个人H2研究。

##  具体内容
### 1. Adaptive Replacement Cache

一种动态，自适应，持续在recency和frequency之间进行平衡的实时自我调节的缓冲算法策略。ARC主要根据策略动态地维护L1和L2两个LRU队列，其中L1包括最近访问，L2包括访问最频繁。具体可以参考代码的实现。


* *org/h2/mvstore/cache/CacheARC.java*
* *org/h2/mvstore/cache/CacheLongKeyARC.java*

#### 参考
*ARC: A SELF-TUNING, LOW OVERHEAD REPLACEMENT CACHE by
Nimrod Megiddo and Dharmendra S. Modha*

### 2. FastStore - 多线程并发事务存储引擎

并发B+树的读写操作要保证串行化的同时，也要保证树遍历的高并发性。尤其对于树结构修改操作（structure modification operation，SMO），如删除page，split节点带来的影响。FastStore实现了page粒度的封锁，并且利用索引粒度的锁来避免SMO带来的影响。另外，为了简化实现，目前FastStore最高支持Read Committed隔离等级。
FastStore使用了crabbing protocol的锁协议：先获取root的锁。然后开始遍历到leaf节点，获取对应child的锁后，如果child节点安全，则释放parent的锁，这样可以避免死锁的同时，也减小了获取锁时间。另外还规定leaf节点只能从左往右上锁。
FastStore实现了latch机制来保证线程安全，latch也有X（互斥）和S（共享）两种模式。

* Search

无论是查找，插入，删除指定row，都要通过Search步骤定位到具体的leaf节点。流程大致是从root节点根据row的Key进行二分查找到具体的child节点，使用crabbing protocol遍历到leaf节点。如果此时其它线程的SMO操作影响了遍历的过程，则需要等待SMO操作完成。SMO操作需要一直持有index的X latch，因此获取Index的S Latch后快速释放latch（Instant Duration），就可以等待SMO操作完成，然后根据需要向上找到没有被修改过的祖先节点，再重新开始search过程。

* Fetch

根据key获取指定的row。Search步骤到达leaf节点后，在该leaf节点中二分查找row，如果没有刚好大于或者等于key的row，则查找其右兄弟节点。如果右兄弟节点是空节点，则是该节点准备删除，因此需要Instant Duration索引latch等待删除完成，然后根据需要向上回溯，重新开始Search步骤。

* Insert

插入row到leaf节点。Search步骤到达leaf节点后，首先查看leaf节点空间是否足够，否则要执行split操作。split操作首先要获得索引的X latch，然后就是不断向上回溯祖先节点插入split后的新key，直到不需要split位置，注意向上回溯的时候避免死锁，必须首先释放child节点的锁，然后再获取parent节点的锁。split之后，释放索引X latch，然后再重新Search到leaf节点，重新执行Insert。

另外要注意，如果root需要split，就是树高度需要增加1，为了避免修改root的pageId，旧的rootPage进行split之后，分配pageId，然后把旧rootPage重新设置为新pageId，再创建新的rootPage设置为当前的rootPageId，以及两个childpage。

* Delete

根据key删除指定row。Search步骤到达leaf节点后，leaf节点删除row，如果leaf节点变为空，则需要删除page。注意删除page需要递归向上回溯删除page对应的key，直到祖先节点非空。同样是先释放child节点的锁，然后再获取parent节点的锁。另外注意，如果left节点非空，但删除的是leaf节点的max key，则需要更新parent节点的key，同理需要递归向上回溯。

* *org/h2/faststore/**
