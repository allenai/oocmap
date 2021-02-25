import oocmap, json, tqdm, itertools

#import time
#time.sleep(20)

def count_ai2(o):
  if isinstance(o, str):
    if "ai2" in o:
      return 1
    else:
      return 0
  if isinstance(o, list) or isinstance(o, oocmap.LazyList) or isinstance(o, tuple) or isinstance(o, oocmap.LazyTuple):
    return sum(count_ai2(x) for x in o)
  if isinstance(o, dict) or isinstance(o, oocmap.LazyDict):
    return sum(count_ai2(k) + count_ai2(v) for k, v in o.items())
  return 0

import sys
setname = sys.argv[1]

if setname.startswith("big_"):
  l = 10000
elif setname.startswith("small_"):
  l = 200000

import timeit
start = timeit.default_timer()

if setname.endswith(".ooc"):
  m = oocmap.OOCMap(setname)
  print(sum(count_ai2(m[i]) for i in tqdm.trange(l, desc="OOC")))
elif setname.endswith(".jsonl"):
  print(sum(count_ai2(json.loads(j)) for j in tqdm.tqdm(itertools.islice(open(setname), l), desc="JSON", total=l)))
elif setname.endswith(".sqlite"):
  from sqlitedict import SqliteDict
  with SqliteDict(setname) as d:
    print(sum(count_ai2(d[i]) for i in tqdm.trange(l, desc="SQLite")))

print(timeit.default_timer() - start)
