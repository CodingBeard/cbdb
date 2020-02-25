package cbdb

import (
	"errors"
	"reflect"
	"strconv"
	"time"
)

type Cache struct {
	cacheProviderConstructor func() CacheProvider
	cache                    CacheProvider
	db                       *GormReadWrite
}

type CacheProvider interface {
	Get(key string) (interface{}, bool)
	Delete(key string)
	Set(key string, value interface{}, ttl time.Duration)
}

type CachableResultModel interface {
	GetCacheKey() string
	GetCacheBucket() string
}

type cachedResult struct {
	queryTime int64
	cache     interface{}
}

type CacheKeyGenerator struct {
	Bucket   string
	Type     interface{}
	Search   CachableResultModel
	Preloads []string
	Wheres   []string
	Order    string
	Limit    int
	Offset   int
	Group    string
	Extra    []string
}

func (k *CacheKeyGenerator) Generate() string {
	var key string
	if k.Type != nil {
		key += reflect.TypeOf(k.Type).String() + ":"
	}
	if k.Search != nil {
		key += k.Search.GetCacheKey() + ":"
	}
	for _, preload := range k.Preloads {
		key += preload + ":"
	}
	for _, where := range k.Wheres {
		key += where + ":"
	}
	key += k.Order + ":"
	if k.Limit > 0 {
		key += strconv.Itoa(k.Limit) + ":"
	}
	if k.Offset > 0 {
		key += strconv.Itoa(k.Offset) + ":"
	}
	key += k.Group + ":"
	for _, extra := range k.Extra {
		key += extra + ":"
	}
	return key
}

type CacheArgs struct {
	Miss   func(db *GormReadWrite, out interface{}) error
	Out    interface{}
	Ttl    time.Duration
	KeyGen CacheKeyGenerator
}

func (c *Cache) FindWhere(out interface{}, where CachableResultModel, ttl time.Duration) error {
	return c.Cache(CacheArgs{
		Miss: func(db *GormReadWrite, out interface{}) error {
			return db.Read().Find(out, where).Error
		},
		Out: out,
		Ttl: ttl,
		KeyGen: CacheKeyGenerator{
			Search: where,
			Extra:  []string{"FindWhere"},
		},
	})
}

func (c *Cache) FirstWhere(out interface{}, where CachableResultModel, ttl time.Duration) error {
	return c.Cache(CacheArgs{
		Miss: func(db *GormReadWrite, out interface{}) error {
			return db.Read().First(out, where).Error
		},
		Out: out,
		Ttl: ttl,
		KeyGen: CacheKeyGenerator{
			Search: where,
			Extra:  []string{"FirstWhere"},
		},
	})
}

func (c *Cache) LastWhere(out interface{}, where CachableResultModel, ttl time.Duration) error {
	return c.Cache(CacheArgs{
		Miss: func(db *GormReadWrite, out interface{}) error {
			return db.Read().Last(out, where).Error
		},
		Out: out,
		Ttl: ttl,
		KeyGen: CacheKeyGenerator{
			Search: where,
			Extra:  []string{"LastWhere"},
		},
	})
}

func (c *Cache) CountWhere(where CachableResultModel, ttl time.Duration) (int64, error) {
	var count int64
	e := c.Cache(CacheArgs{
		Miss: func(db *GormReadWrite, out interface{}) error {
			return db.Read().Model(where).Where(where).Count(out).Error
		},
		Out: &count,
		Ttl: ttl,
		KeyGen: CacheKeyGenerator{
			Search: where,
			Extra:  []string{"CountWhere"},
		},
	})

	return count, e
}

func (c *Cache) SaveFlush(model CachableResultModel) error {
	e := c.db.Write().Save(model).Error
	if e != nil {
		return e
	}

	return c.Flush(CacheKeyGenerator{Bucket: model.GetCacheBucket()})
}

func (c *Cache) Cache(args CacheArgs) error {
	valueOfOut := reflect.ValueOf(args.Out)
	if valueOfOut.Kind() != reflect.Ptr {
		return errors.New("out is not a pointer")
	}
	if args.KeyGen.Type == nil {
		args.KeyGen.Type = args.Out
	}
	key := args.KeyGen.Generate()

	if key == "" {
		return errors.New("cannot cache with an empty KerGen key, add some information to the KeyGen")
	}

	bucket := args.KeyGen.Bucket
	if bucket == "" && args.KeyGen.Search != nil {
		bucket = args.KeyGen.Search.GetCacheBucket()
	}

	var cache CacheProvider
	if bucket != "" {
		if x, ok := c.cache.Get(bucket); ok {
			if bucket, ok := x.(CacheProvider); ok {
				cache = bucket
			} else {
				cache = c.cache
			}
		} else {
			cache = c.cacheProviderConstructor()
			c.cache.Set(bucket, cache, time.Hour*24)
		}
	} else {
		cache = c.cache
	}

	now := time.Now().UnixNano()

	if x, ok := cache.Get(key); ok {
		if result, ok := x.(*cachedResult); ok {
			if result.queryTime > now-int64(args.Ttl) {
				reflect.Indirect(valueOfOut).Set(reflect.ValueOf(result.cache).Elem())
				return nil
			}
		} else {
			return errors.New("error casting to cachedResult pointer type")
		}
	}

	e := args.Miss(c.db, args.Out)
	if e != nil {
		return e
	}

	cache.Set(key, &cachedResult{queryTime: now, cache: args.Out}, args.Ttl)

	return nil
}

func (c *Cache) Flush(keyGen CacheKeyGenerator) error {
	var cache CacheProvider
	bucket := keyGen.Bucket
	if bucket == "" && keyGen.Search != nil {
		bucket = keyGen.Search.GetCacheBucket()
	}
	if bucket != "" {
		if x, ok := c.cache.Get(bucket); ok {
			if bucket, ok := x.(CacheProvider); ok {
				cache = bucket
			} else {
				cache = c.cache
			}
		} else {
			cache = c.cache
		}
	} else {
		cache = c.cache
	}

	key := keyGen.Generate()

	if key != "" {
		cache.Delete(key)
	} else {
		c.cache.Delete(bucket)
	}

	return nil
}
