package cacher_test

import (
	"math/rand"
	"nyadb2/backend/utils"
	"nyadb2/backend/utils/cacher"
	"sync"
	"testing"
)

func TestCacher2(t *testing.T) {
	mockGet := func(uid utils.UUID) (interface{}, error) {
		return uid, nil
	}
	mockRelease := func(underlying interface{}) {
	}

	options := new(cacher.Options)
	options.Get = mockGet
	options.Release = mockRelease
	options.MaxHandles = 2

	c := cacher.NewCacher(options)

	for i := 0; i < 1000; i++ {
		_, err := c.Get(utils.UUID(i % 2))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCacher(t *testing.T) {
	mockGet := func(uid utils.UUID) (interface{}, error) {
		return uid, nil
	}
	mockRelease := func(underlying interface{}) {
	}

	options := new(cacher.Options)
	options.Get = mockGet
	options.Release = mockRelease
	options.MaxHandles = 50

	c := cacher.NewCacher(options)

	wg := sync.WaitGroup{}
	wg.Add(200)
	worker := func() {
		for i := 0; i < 1000; i++ {
			u32 := rand.Uint32()
			uid := utils.UUID(u32)
			h, err := c.Get(uid)
			if err == cacher.ErrCacheFull {
				continue
			}
			tmp := h.(utils.UUID)
			if tmp != uid {
				t.Fatal("error")
			}
			c.Release(tmp)
		}
		wg.Done()
	}

	for i := 0; i < 200; i++ {
		go worker()
	}

	wg.Wait()
}
