OOCMap
======

OOCMap is a Python dictionary that's backed by disk. You use it like this:

```Python
from oocmap import OOCMap

m = OOCMap(filename)
m[1] = "John"
m[2] = "Paul"
m[3] = [4, "score", "and", (7.0, "years"), "ago"]
m["four"] = m[1]

# restart python

m = OOCMap(filename)
assert m[1] == "John"
```

OOCMap has a few headline features:
 * It is fast. Accessing data in OOCMap is about 50% as fast as data in main memory.
   That's very fast for a serialization format. (Writing is much slower compared to memory though).
 * The total size of the map can exceed the size of main memory.
 * All data access is lazy. This is true not just for the top-level items in the map, but for *all data in the map*. If
   you access `m[3]` in the example above, it returns a `LazyList` instance, which won't cause disk access until you
   request a member from the list.
 * Multiple Python processes can access the same OOCMap at the same time.
 * Immutable values are automatically de-duplicated. If your data has a lot of repeated strings in it (this is the most 
   common case), you could save some space.
 * Floats are stored in native precision (as opposed to storing them in JSON, which is lossy)

Keys can be any immutable Python type (same as regular dictionaries).
Values can be any Python type (see below for exceptions).

Getting Started
---------------

Until I figure out how to get this thing to PyPi, you have to install from source:
```
git clone https://github.com/allenai/oocmap.git
cd oocmap
pip install -e .
```

Limitations
-----------

We're still missing some types that are commonly requested. Please make a PR if you urgently need these!
 * Python `bytes` and `bytearray`
 * Python `complex`
 * Python `set` and `frozenset`
 * Numpy arrays
 * Torch/Tensorflow/Jax tensors

Also, there is no garbage collector. You can delete things out of OOCMap, but the file backing it will never shrink.
At a minimum, we should have a "vacuum" method that locks the whole map while it finds and deletes lost data.
