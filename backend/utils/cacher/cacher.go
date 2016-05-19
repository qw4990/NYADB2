/*
	cacher.go 实现了一个带reference的cache.
	Cacher被用在了多个地方, 如Pcacher, DM对Dataitem的缓存, VM对Entry的缓存等.
*/
package cacher

import (
	"errors"
	"nyadb2/backend/utils"
	"sync"
	"time"
)

var (
	ErrCacheFull = errors.New("Cache is full.")

	_TIME_WAIT = time.Millisecond
)

type Cacher interface {
	Get(uid utils.UUID) (interface{}, error)
	Release(uid utils.UUID)
	Close()
}

type Options struct {
	// 当uid不在缓存中时, 则调用该函数取得对应资源.
	// 该函数必须要是并发安全的.
	Get func(uid utils.UUID) (interface{}, error)

	// 释放资源的行为.
	Release func(underlying interface{})

	// 允许的最大资源数, 0表示无限.
	MaxHandles uint32
}

func NewCacher(options *Options) *cacher {
	return &cacher{
		options: options,
		cache:   make(map[utils.UUID]interface{}),
		getting: make(map[utils.UUID]bool),
		refs:    make(map[utils.UUID]uint32),
	}
}

type cacher struct {
	options *Options

	cache   map[utils.UUID]interface{}
	refs    map[utils.UUID]uint32
	getting map[utils.UUID]bool // 该map表示正在拿去, 但还未成功的资源.
	count   uint32              // cache中handle个数
	lock    sync.Mutex          // lock保护了上面所有变量
}

func (c *cacher) Get(uid utils.UUID) (interface{}, error) {
	for {
		c.lock.Lock()
		if _, ok := c.getting[uid]; ok {
			// 如果请求的资源正在被其他线程获取, 则等待那个线程获取结束.
			c.lock.Unlock()
			time.Sleep(_TIME_WAIT)
			continue
		}

		if _, ok := c.cache[uid]; ok {
			// 如果资源在缓存中, 则直接返回
			h := c.cache[uid]
			c.refs[uid]++
			c.lock.Unlock()
			return h, nil
		}

		// 否则, 则尝试获取该资源
		if c.options.MaxHandles > 0 && c.count == c.options.MaxHandles {
			// 资源数已经满
			c.lock.Unlock()
			return nil, ErrCacheFull
		} else {
			c.count++             // 为马上要新建的handle预留一段空间.
			c.getting[uid] = true // 并标记该资源ID
		}
		c.lock.Unlock()
		break
	}

	// 注意调用options.Get时是无锁的, 因此能够和其他的Get并发进行.
	// 这也要求options.Get是并发安全的.
	underlying, err := c.options.Get(uid)
	if err != nil {
		c.lock.Lock()
		c.count--
		delete(c.getting, uid)
		c.lock.Unlock()
		return nil, err
	}

	c.lock.Lock()
	delete(c.getting, uid)
	c.cache[uid] = underlying
	c.refs[uid] = 1
	c.lock.Unlock()

	return underlying, nil
}

func (c *cacher) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	// TODO: 如果还有c.getting不为空?
	for uid, h := range c.cache {
		c.options.Release(h)
		delete(c.refs, uid)
		delete(c.cache, uid)
	}
}

func (c *cacher) Release(uid utils.UUID) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.refs[uid]--
	if c.refs[uid] == 0 {
		underlying := c.cache[uid]
		/*
			这里的Release是不能被异步处理的.
			如果将Release异步处理, 那么有可能在Release为完成之前, 就有新的线程Get这个资源,
			那么新线程得到的将会是未被更新的新资源.
		*/
		c.options.Release(underlying)
		delete(c.refs, uid)
		delete(c.cache, uid)
		c.count--
	}
}
