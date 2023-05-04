Overview
========
:doc:`cache3 <index>` supports both **memory** and **disk** cache backends.

**memory-based** cache has good performance but does not support data persistence.

**disk-based** caches support persistence but have slightly lower performance.

Regardless of which cache is used, they all follow the same API, which means that the backend implementation of the cache can be switched at will without cost.

The built-in cache policies are LRU, LFU, FIFO, and if these don't meet your needs, you can extend them with `manager`.


.. _installation:

Installation
============


To use cache3, first install it using pip:

.. code-block:: console

   $ pip install -U cache3

or download source and install

.. code-block:: console

    $ git clone https://github.com/StKali/cache3.git
    $ cd cache3
    $ python setup.py install


Quick start
===========

Creating a cache is so easy, and all cache parameters are set to conservative defaults. Default is a good choice if you don't want too much detail, but it's not guaranteed to be efficient.

In class docs you can know more detailed parameters and configuration information.

.. code-block:: python

   In [1]: from cache3 import SafeCache
   In [2]: cache = SafeCache()

   # Default cache name: `default.cache3`, timeout:300s
   In [3]: cache
   Out[3]: <SafeCache name=default.cache3 timeout=300.00>

   # Default max size  1 << 24 (16M)
   In [4]: cache.max_size
   Out[4]: 16777216

   # Default key evict policy
   In [5]: cache.evict
   Out[5]: 'lru_evict'

set, get, delete(del), has_key(in)
----------------------------------

set an item, get a value, and delete a key using the usual operators:

.. code-block:: python

    In [6]: cache['name'] = 'clark monkey'

    In [7]: cache['name']     # get name
    Out[7]: 'clark monkey'

    In [8]: del cache['name']     # delete key from cache

    In [9]: 'name' in cache    # default tag
    Out[9]: False



The dictionary-like operation is simple, but the **tag** and **timeout** cannot be specified.
If you want to specify these parameters, :meth:`get<cache3.BaseCache.get>`, :meth:`set<cache3.BaseCache.set>`, :meth:`ex_set<cache3.BaseCache.ex_set>`, :meth:`delete <cache3.BaseCache.delete>` are good choices.

.. code-block:: python

    # set item
    In [10]: cache.set('name', 'venus')
    Out[10]: True

    # get ttl
    In [11]: cache.ttl('name')
    Out[11]: 299.01095983695984

    # Get value
    In [12]: cache.get('name')
    Out[12]: 'venus'

    # Delete
    In [13]: cache.delete('name')
    Out[13]: True

    # Has key
    In [14]: cache.has_key('name')
    Out[14]: False

Use tag to group keys, which allows key duplication.

.. code-block:: python

    # Set item with tag
    In [15]: cache.set('name', 'venus', tag='class:1')
    Out[15]: True

    In [16]: cache.set('name', 'apollo', tag='class:2')
    Out[16]: True

    # If tag is not specified, the value cannot be found correctly
    # Tag is similar to namespace, keys are divided into spaces by tag
    In [17]: cache.get('name')

    In [18]: cache.get('name', tag='class:1')
    Out[18]: 'venus'

    In [19]: cache.get('name', tag='class:2')
    Out[19]: 'apollo'

    # Set item with timeout
    In [20]: cache.set('count', 30, timeout=60, tag='class:1')
    Out[20]: True

    In [21]: cache.ttl('count', tag='class:1')
    Out[21]: 59.076417922973633



ex_set
------

It ensures **set** is safety through exclusive locks. :class:`SafeCache <cache3.SafeCache>` uses `threading.Lock <https://docs.python.org/3/library/threading.html#threading.Lock>`_, and :ref:`disk-based cache <disk-based>` uses file locks, so they are process-safe.

.. code-block:: python

    # clear the cache
    In [22]: cache.clear()

    In [23]: cache.set('name', 'venus')
    Out[23]: True

    # Mutex set item
    In [24]: cache.ex_set('name', 'apollo')
    Out[24]: False

    # Delete the item and try again
    In [25]: cache.delete('name')
    Out[25]: True

    # Reset success
    In [26]: cache.ex_set('name', 'apollo')
    Out[26]: True


get_many
--------

Get many items at one time, support tag parameter, and only one tag can be specified

.. code-block:: python

    # Clear the cache
    In [27]: cache.clear()

    # Set items
    In [28]: for i in range(3):
        ...:     cache.set(i, i, tag='test:get_many')
        ...:
    In [29]: cache.get_many([i for i in range(3)], tag='test:get_many')
    Out[29]: {0: 0, 1: 1, 2: 2}



memoize
-------

.. note::

    :meth:`memoize <cache3.BaseCache.memoize>` This decorator is insensitive to parameters.

.. code-block:: python

    from cache3 import SimpleCache

    cache: SimpleCache = SimpleCache()

    @cache.memoize(timeout=10, tag='cached:page')
    def query_pages() -> bytes:
        return b'<h1> Hello Cache3 </h1>'

    # Note: This decorator is insensitive to parameters.


inspect
-------

:meth:`inspect <cache3.BaseCache.inspect>` can obtain almost all the information of the key. Because the storage backend is unknown, there are differences between different implementations.

- Memory backend

.. code-block:: python

    #  Based memory cache
    In [1]: cache: SafeCache = SafeCache()
    In [2]: cache['name'] = 'Venus'
    In [3]: cache.inspect('name')
    Out[3]: {
     'key': 'name',
     'store_key': 'name:default',
     'store_value': 'Venus',
     'value': 'Venus',
     'expire': 1644718648.995299
    }

- Disk backend

.. code-block:: python

    #  Based disk cache
    In [1]: cache: DiskCache = DiskCache()
    In [2]: cache['name'] = 'Ares'
    In [3]: cache.inspect('name')
    Out[3]: {
        'key': 'name',
        'store': 1644718388.4478312,
        'expire': 1644718688.4478312,
        'access': 1644718388.4478312,
        'access_count': 0,
        'tag': 'default',
        'value': 'cache3',
        'store_key': 'name',
        'serial_value': 'Ares'
    }



others
------

Some APIs that are not commonly used but are very useful: :meth:`ttl <cache3.BaseCache.ttl>`, :meth:`touch <cache3.BaseCache.touch>`, :meth:`clear <cache3.BaseCache.clear>`


.. code-block:: python

    # Get the ttl
    In [1]: cache.ttl('name')
    Out[1]: 297.9396250247955

    # touch
    # Touch the key and reset ttl
    In [2]: cache.touch('name', 100)
    Out[2]: True
    In [3]: cache.ttl('name')
    Out[3]: 98.66487669944763


iterable
--------



.. code-block:: python

    # It's iterable.
    In [1]: for i in range(3):
    ...:     cache.set(i, i, tag='test:get_many')
    ...:
    In [2]: list(cache)
    Out[2]: [(0, 0, 'default'), (1, 1, 'default'), (2, 2, 'default')]

    In [3]: tuple(cache)
    Out[3]: ((0, 0, 'default'), (1, 1, 'default'), (2, 2, 'default'))




.. _memory-based:

memory-based cache
==================


Memory-based caches will completely lose the data in the cache when the program crashes or exits, in other words, they do not support data persistence.

MiniCache
-----------

:class:`SimpleCache <cache3.SimpleCache>` is a thread-unsafe cache, which aims to provide high performance but does not guarantee data safety under multi-threading. :class:`SafeCache <cache3.SafeCache>` is a good choice if you want thread safety.


Cache
---------

:class:`SafeCache <cache3.SafeCache>` is a thread-safe cache. It has exactly the same implementation as :class:`SimpleCache <cache3.SimpleCache>`, based on Python's `OrderedDict <https://docs.python.org/3/library/collections.html#collections.OrderedDict>`_, the difference is the type of Lock, :class:`SimpleCache <cache3.SimpleCache>` Lock is an empty lock, while  :class:`SafeCache <cache3.SafeCache>` uses `threading.Lock <https://docs.python.org/3/library/threading.html#threading.Lock>`_ to ensure its thread safety.




.. _disk-based:

disk-based cache
================


The disk-based cache backend is implemented in `SQLite3 <https://www.sqlite.org/index.html>`_ because it is lightweight enough and performs well.

.. note::

    Since the disk cache is based on `SQLite3 <https://www.sqlite.org/index.html>`_, even after a series of optimizations, it still needs to be carefully considered whether it will become a concurrency bottleneck. In fact, in most cases it is sufficient.

DiskCache
---------

:class:`DiskCache<cache3.DiskCache>` overrides :class:`SimpleDiskCache <cache3.SimpleDiskCache>`'s :meth:`serialize() <cache3.BaseCache.serialize>` and :meth:`deserialize() <cache3.BaseCache.deserialize>` methods by inheriting :class:`PickleMixin <cache3.PickleMixin>`  mixins to support more data types, but has no difference with :class:`SimpleDiskCache <cache3.SimpleDiskCache>`.

.. code-block:: python

   class DiskCache(PickleMixin, SimpleDiskCache):


- :class:`JsonDiskCache <cache3.JsonDiskCache>`
