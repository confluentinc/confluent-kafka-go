package kafka

import (
	"container/list"
	"sync"
)

type SimpleLRU interface {
	Add(key, value interface{}) (*Node)
	Get(key interface{}) (node *Node, ok bool)
	Purge() (int)
}

type Queue struct {
	sync.Mutex
	frames *list.List
}

type EventHook func(key, value interface{})

type lru struct {
	limit int
	// sync.Map wasn't added until 1.9
	sync.Mutex
	tenants *list.List
	index map[interface{}]*list.Element
	onEvict EventHook
}

type Node struct {
	Key interface{}
	Value interface{}
}

func NewSimpleLRU(size int, onEvict EventHook) (*lru) {
	return &lru{
		limit : size,
		tenants : list.New(),
		index : make(map[interface{}]*list.Element),
		onEvict : onEvict,
	}
}

func (l *lru) Add(key, value interface{}) (*Node) {
	l.Lock()

	if l.tenants.Len() >= l.limit {
		l.evict(l.tenants.Back())
	}

	node := &Node{key, value}
	l.index[key] = l.tenants.PushFront(node)

	l.Unlock()

	return node
}

func (l *lru) Get(key interface{}) (node *Node, ok bool) {
	l.Lock()
	defer l.Unlock()

	if elm, ok := l.index[key]; ok {
		l.tenants.MoveToFront(elm)
		return elm.Value.(*Node), ok
	}

	return nil, ok
}

func (l *lru) Purge() (purged int) {
	l.Lock()
	purged = l.tenants.Len()

	l.tenants.Init()
	l.index = make(map[interface{}]*list.Element)

	l.Unlock()

	return purged
}

func (l *lru) evict(e *list.Element) {
	t := l.tenants.Remove(e).(*Node)

	if l.onEvict != nil {
		l.onEvict(t.Key, t.Value)
	}

	delete(l.index, t.Key)
}

