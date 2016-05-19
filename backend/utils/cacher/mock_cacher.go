package cacher

import (
	"nyadb2/backend/utils"
	"sync"
)

/*
	mockCacher 只支持单线程的Get操作, 其他和Cacher一样.
*/
type mockCacher struct {
	options *Options
	refs    map[utils.UUID]int
	cache   map[utils.UUID]interface{}
	count   uint32
	lock    sync.Mutex
}

func NewMockCacher(options *Options) *mockCacher {
	return &mockCacher{
		options: options,
		refs:    make(map[utils.UUID]int),
		cache:   make(map[utils.UUID]interface{}),
	}
}

func (mc *mockCacher) Get(uid utils.UUID) (interface{}, error) {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	if _, ok := mc.cache[uid]; ok {
		mc.refs[uid]++
		return mc.cache[uid], nil
	}

	if mc.options.MaxHandles > 0 && mc.count == mc.options.MaxHandles {
		return nil, ErrCacheFull
	}

	h, err := mc.options.Get(uid)
	if err != nil {
		return nil, err
	}

	mc.cache[uid] = h
	mc.count++
	mc.refs[uid] = 1

	return h, nil
}

func (mc *mockCacher) Release(uid utils.UUID) {
	mc.lock.Lock()
	defer mc.lock.Unlock()

	mc.refs[uid]--
	if mc.refs[uid] == 0 {
		mc.options.Release(mc.cache[uid])
		delete(mc.refs, uid)
		delete(mc.cache, uid)
		mc.count--
	}
}

func (mc *mockCacher) Close() {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	for uid, h := range mc.cache {
		mc.options.Release(h)
		delete(mc.refs, uid)
		delete(mc.cache, uid)
	}
}
