#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023/2/15
# author: clarkmonkey@163.com

from shutil import rmtree
from time import sleep, time as current
from typing import Any, List, Union

import pytest

from cache3.util import empty
from cache3 import MiniCache, LazyMiniCache, MiniDiskCache, LazyMiniDiskCache, Cache, LazyCache, DiskCache, LazyDiskCache
from utils import rand_strings, rand_string

params = pytest.mark.parametrize
raises = pytest.raises

not_found: str = 'not found'
all_types: List[Any] = [

    # float
    99999999999.,
    0.0000000001,
    -1.1,
    
    # integer
    1,
    0,
    -1,  
    
    # strng
    '',
    'key',
    
    # none
    None,
    
    # bool
    True,
    False,
    
    # empty
    empty,
    
    # tuple
    (1, 2),
    
    # class
    int,
    float,
    
    # function
    type,
    any,
]

MiniFamily = Union[MiniCache, LazyMiniCache, MiniDiskCache, LazyMiniDiskCache]
CacheFamily = Union[Cache, LazyCache, DiskCache, LazyDiskCache]
class TestBaseMiniCacheAPI:
    
    def setup_class(self) -> None:
        self.caches: List[MiniFamily] = [
            MiniCache('MiniCacheInstance'),
            LazyMiniCache('LazyMiniCacheInstance'),
            MiniDiskCache('MiniDiskCacheInstance'),
            LazyMiniDiskCache('LazyMiniDiskCacheInstance')
        ]
        
    def teardown_class(self) -> None:
        for cache in self.caches:
            if hasattr(cache, 'directory'):
                rmtree(cache.directory)

    def setup_method(self) -> None:
        assert all((cache.clear() for cache in self.caches))
    
    @params('key, value', ((t, t) for t in all_types))
    def test_set(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache.set(key, value)
            assert cache.pop(key) == value
            cache.set(value, key)
            assert cache.pop(value) == key
            cache.set(key, value, timeout=-1)
            assert not cache.exists(key)

    @params('key, value', ((t, t) for t in all_types))
    def test_ex_set(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            assert cache.ex_set(key, value)
            assert not cache.ex_set(key, value)
            assert cache.touch(key, timeout=-1)
            assert cache.ex_set(key, value)
            assert cache.get(key) == value
    
    @params('key, value', ((t, t) for t in all_types))
    def test_get(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache.set(key, value)
            assert cache.get(key, not_found) == value
            assert cache.get(key) == value
            assert cache.delete(key)
            assert cache.get(key, not_found) == not_found

    def test_get_many(self) -> None:
        test_keys = list(rand_strings(10))
        for cache in self.caches:
            for key in test_keys:
                cache[key] = key
            assert cache.delete(test_keys[0])
            kvs: dict = cache.get_many(*test_keys)   
            assert len(kvs) == len(test_keys) - 1
            assert list(sorted(test_keys[1:])) == list(sorted(kvs.keys()))
            for k, v in kvs.items():
                assert k == v

    def test_incr_decr(self) -> None:
        count = 'count'
        for cache in self.caches:
            assert cache.set(count, 0)
            assert cache.incr(count, 1) == 1
            assert cache.decr(count, 1) == 0
            
            with raises(KeyError, match='key .* not found in cache'):
                cache.incr('no-existed')
            
            cache.set('not-number', 'a')
            with raises(TypeError, match='unsupported operand type'):
                cache.incr('not-number') 

    def test_ttl(self) -> None:
        for cache in self.caches:
            cache.set('key1', 'value', timeout=100)
            assert cache.ttl('key1') > 99
            # fix #16 ttl returns the expired
            assert cache.ttl('key1') <= 100
            assert not cache.has_key('key2')
            cache.set('key2', 'value')
            assert cache.ttl('key2') is None
            assert cache.ttl('not-existed') == -1
    
    @params('key, value', ((t, t) for t in all_types))
    def test_delete(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            assert cache.set(key, value)
            assert cache.delete(key)
            assert cache.get(key, not_found) == not_found

    @params('key, value', ((t, t) for t in all_types))
    def test_inspect(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache.set(key, value, timeout=100)
            assert cache.inspect(not_found) is None
            ins: dict = cache.inspect(key)
            assert isinstance(ins, dict)
            assert ins['key'] == key
            assert ins['value'] == value
            assert ins['expire'] - current() > 99
            assert 100 >= ins['ttl'] >= 99

    @params('key, value', ((t, t) for t in all_types))
    def test_pop(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache.set(key, value)
            assert cache.exists(key)
            assert cache.pop(key) == value
            assert cache.pop(key, default=not_found) == not_found
    
    @params('key, value', ((t, t) for t in all_types))
    def test_exists(self, key, value) -> None:
        for cache in self.caches:
            cache.set(key, value)
            assert cache.exists(key)
            cache.delete(key)
            assert not cache.exists(key)
    
    def test_touch(self) -> None:
        key: str = 'touch_key'
        for cache in self.caches:
            cache.set(key, key, timeout=10)
            assert 10 > cache.ttl(key) > 9
            assert cache.touch(key, 100)
            assert 100 > cache.ttl(key) > 99
            assert not cache.touch(not_found)

    def test_keys(self) -> None:
        count: int = 104
        keys: List[str] = list(rand_strings(count, 4, 20))
        for cache in self.caches:
            for key in keys:
                cache.set(key, rand_string(4, 20))
            assert len(cache) == count
            keys.sort()
            assert list(sorted(cache.keys())) == keys

    def test_values(self) -> None:
        count: int = 104
        values: List[str] = list(rand_strings(count, 4, 20))
        for cache in self.caches:
            for value in values:
                cache.set(rand_string(4, 20), value)
            assert len(cache) == count
            values.sort()
            assert list(sorted(cache.values())) == values

    def test_items(self) -> None:
        count: int = 104
        items: Dict[str, str] = {rand_string(): rand_string for i in range(count)}
        for cache in self.caches:
            for k, v in items.items():
                cache.set(k, v)
        
        cache_items: Dict[str, str] = {k: v for k, v in cache.items()}
        assert len(cache_items) == len(items)
        for k, v in items.items():
            assert cache_items.get(k) == v

    def test_clear(self) -> None:
        for cache in self.caches:
            cache.clear()
            assert len(cache) == 0
            cache.set('k', 'v')
            assert len(cache) == 1
            assert cache.clear()
            assert len(cache) == 0

    def test_memoize(self) -> None:
        for cache in self.caches:
            count = 1    
            @cache.memoize(0.1)
            def cal():
                nonlocal count
                count += 1
                return count
            assert cal() == 2
            assert cal() == 2
            assert cal() == 2
            sleep(0.11)
            assert cal() == 3

    def test_failed_memoize(self) -> None:
        for cache in self.caches:
            with raises(TypeError, match='The `memoize` decorator should be called with a `timeout` parameter.'):
                @cache.memoize
                def error():
                    ...                
                error()
     
    @params('key, value', ((t, t) for t in all_types))
    def test_setitem(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache[key] = value
            assert cache.get(key) == value
    
    @params('key, value', ((t, t) for t in all_types))
    def test_delitem(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache[key] = value
            assert cache.get(key) == value
            del cache[key]
            assert not cache.exist(key)

    @params('key, value', ((t, t) for t in all_types))
    def test_delitem(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache[key] = value
            assert cache[key] == value


class TestBaseCacheAPI:
        
    def setup_class(self) -> None:
        self.caches: List[CacheFamily] = [
            Cache('CacheInstance'),
            LazyCache('LazyCacheInstance'),
            DiskCache('DiskCacheInstance'),
            LazyDiskCache('LazyDiskCacheInstance')
        ]
        
    def teardown_class(self) -> None:
        for cache in self.caches:
            if hasattr(cache, 'directory'):
                rmtree(cache.directory)

    def setup_method(self) -> None:
        assert all((cache.clear() for cache in self.caches))
        
    @params('key, value', ((t, t) for t in all_types))
    def test_set(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            
            cache.set(key, value)
            assert cache.pop(key) == value
            
            cache.set(value, key)
            assert cache.pop(value) == key

            cache.set(key, value, tag='tag1')
            assert cache.get(key, tag='tag1') == value
            assert cache.get(key, tag='tag2', default=not_found) == not_found

            assert cache.exists(key, tag='tag1')
            
            cache.set(key, value, timeout=0.01, tag='tm')
            assert cache.exists(key, tag='tm')
            sleep(0.011)
            assert not cache.exists(key, tag='tm')
    
    @params('key, value', ((t, t) for t in all_types))
    def test_ex_set(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            assert cache.ex_set(key, value)
            assert not cache.ex_set(key, value)
            assert cache.ex_set(key, value, tag='t1')
            assert cache.ex_set(key, value, tag='t2')
            assert not cache.ex_set(key, value, tag='t2')
    
    @params('key, value', ((t, t) for t in all_types))
    def test_get(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache.set(key, value)
            assert cache.get(key, not_found) == value
            assert cache.get(key) == value
            assert cache.delete(key)
            assert cache.get(key, not_found) == not_found

            cache.set(key, value, tag='t1')
            assert cache.get(key, not_found) == not_found
            assert cache.get(key, tag='t1') == value
    
    def test_get_many(self) -> None:
        test_keys: List[str] = list(rand_strings(10))
        for cache in self.caches:
            for index, key in enumerate(test_keys):
                if index % 2:
                    cache.set(key, key, tag='t1')
                else:
                    cache.set(key, key, tag='t2')

            kvs: dict = cache.get_many(*test_keys, tag='t2')   
            assert len(kvs) == len(test_keys[::2])
            assert list(sorted(test_keys[::2])) == list(sorted(kvs.keys()))
            for k, v in kvs.items():
                assert k == v
    
    def test_incr_decr(self) -> None:
        count: str = 'count'
        for cache in self.caches:
            assert cache.set(count, 0, tag=1)
            assert cache.incr(count, 1, tag=1) == 1
            assert cache.decr(count, 1, tag=1) == 0
            
            with raises(KeyError, match='key .* not found in cache'):
                cache.incr(count)
            
            cache.set(count, 'a')
            with raises(TypeError, match='unsupported operand type'):
                cache.incr(count)
    
    def test_ttl(self) -> None:
        key: str = 'ttl_key'
        value: str = 'ttl_value'
        for cache in self.caches:
            cache.set(key, value, timeout=100, tag=1)
            assert 100 >= cache.ttl(key, tag=1) > 99
            assert cache.ttl(key) == -1
            assert not cache.has_key(key)
            cache.set(key, value)
            assert cache.ttl(key) is None

    @params('key, value', ((t, t) for t in all_types))
    def test_delete(self, key: Any, value: Any) -> None:

        for cache in self.caches:
            assert cache.set(key, value, tag=1)
            assert not cache.exists(key)
            assert not cache.delete(key)
            assert cache.exists(key, tag=1)
            assert cache.delete(key, tag=1)
            assert cache.get(key, not_found, tag=1) == not_found

    @params('key, value', ((t, t) for t in all_types))
    def test_inspect(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache.set(key, value, timeout=100, tag=1)
            assert cache.inspect(key) is None
            ins: dict = cache.inspect(key, tag=1)
            assert isinstance(ins, dict)
            assert ins['key'] == key
            assert ins['value'] == value
            assert ins['expire'] - current() > 99
            assert 100 >= ins['ttl'] >= 99      
    
    @params('key, value', ((t, t) for t in all_types))
    def test_pop(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache.set(key, value, tag=1)
            assert cache.exists(key, tag=1)
            assert not cache.exists(key)
            assert cache.pop(key, default=not_found) == not_found
            assert cache.pop(key) is None
            assert cache.pop(key, tag=1) == value
            
    @params('key, value', ((t, t) for t in all_types))
    def test_exists(self, key, value) -> None:
        for cache in self.caches:
            cache.set(key, value, tag=1)
            assert cache.exists(key, tag=1)
            assert not cache.exists(key)
            cache.delete(key, tag=1)
            assert not cache.exists(key, tag=1)

    def test_touch(self) -> None:
        key: str = 'touch_key'
        for cache in self.caches:
            cache.set(key, key, timeout=10, tag=1)
            assert 10 > cache.ttl(key, tag=1) > 9
            assert cache.touch(key, 100, tag=1)
            assert 100 > cache.ttl(key, tag=1) > 99
            assert not cache.touch(not_found, tag=1)
            
    def test_keys(self) -> None:
        count = 104
        keys = list(rand_strings(count, 4, 20))
        for cache in self.caches:
            for key in keys:
                cache.set(key, rand_string(4, 20), tag=1)
            assert len(cache) == count
            keys.sort()
            assert len(list(sorted(cache.keys()))) == 0
            assert list(sorted(cache.keys(tag=1))) == keys
    
    def test_values(self) -> None:
        count = 104
        values = list(rand_strings(count, 4, 20))
        for cache in self.caches:
            for value in values:
                cache.set(rand_string(4, 20), value, tag=1)
            assert len(cache) == count
            values.sort()
            assert len(list(sorted(cache.values()))) == 0
            assert list(sorted(cache.values(tag=1))) == values
            
    def test_items(self) -> None:
        count = 104
        items: Dict[str, str] = {rand_string(): rand_string for i in range(count)}
        for cache in self.caches:
            for k, v in items.items():
                cache.set(k, v, tag=1)
        
        cache_items = {k: v for k, v in cache.items(tag=1)}
        assert len(cache_items) == len(items)
        for k, v in items.items():
            assert cache_items.get(k) == v

    def test_clear(self) -> None:
        for cache in self.caches:
            cache.clear()
            assert len(cache) == 0
            cache.set('k', 'v', tag=1)
            assert len(cache) == 1
            assert cache.clear()
            assert len(cache) == 0

    def test_memoize(self) -> None:

        for cache in self.caches:
            count = 1    
            @cache.memoize(0.1, tag=1)
            def cal():
                nonlocal count
                count += 1
                return count

            assert cal() == 2
            assert cal() == 2
            assert cal() == 2
            sleep(0.11)
            assert cal() == 3

    def test_failed_memoize(self) -> None:

        for cache in self.caches:
            with raises(TypeError, match='The `memoize` decorator should be called with a `timeout` parameter.'):
                @cache.memoize
                def error():
                    ...                
                error()
     
    @params('key, value', ((t, t) for t in all_types))
    def test_setitem(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache[key] = value
            assert cache.get(key) == value
    
    @params('key, value', ((t, t) for t in all_types))
    def test_delitem(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache[key] = value
            assert cache.get(key) == value
            del cache[key]
            assert not cache.exist(key)

    @params('key, value', ((t, t) for t in all_types))
    def test_delitem(self, key: Any, value: Any) -> None:
        for cache in self.caches:
            cache[key] = value
            assert cache[key] == value
            

class TestEvictLogic:
    
    def setup_class(self) -> None:
        self.caches: List[MiniFamily] = [
            MiniCache('MiniCacheInstance'),
            LazyMiniCache('LazyMiniCacheInstance'),
            MiniDiskCache('MiniDiskCacheInstance'),
            LazyMiniDiskCache('LazyMiniDiskCacheInstance')
        ]
        
        
    def teardown_class(self) -> None:
        for cache in self.caches:
            if hasattr(cache, 'directory'):
                rmtree(cache.directory)
                
    def teadown_method(self) -> None:
        for cache in self.caches:
            cache.clear()
    
    def test_fifo(self) -> None:   
        for cache in self.caches:
            cache.evict = 'fifo'
            cache.max_size = 10
            for i in range(12):
                cache.set(i, i)
            assert len(cache) <= cache.max_size
            assert not cache.exists(0)
            assert not cache.exists(1)
            
    def test_lru(self) -> None:
        for cache in self.caches:
            cache.clear()
            cache.evict = 'lru'
            cache.max_size = 10
            for i in range(10):
                cache.set(i, i)
            assert len(cache) == 10
            cache.get(1)
            assert cache.exists(0)
            cache.set(11, 11)
            assert not cache.exists(0)
            
            # cache.get(3)
            # cache.get(5)
            # cache.set(11)
            # cache.set(13)
            # cache.set(15)
            

    def test_lfu(self) -> None:
        for cache in self.caches:
            cache.evict = 'lfu'
    
    def test_default(self) -> None:
        for cache in self.caches:
            cache.evict = 'default'
