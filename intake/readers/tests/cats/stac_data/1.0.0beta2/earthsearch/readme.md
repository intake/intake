Generated with:

```python
import satsearch
import pystac
import json

bbox = [35.48, -3.24, 35.58, -3.14]
dates = '2020-07-01/2020-08-15'
URL='https://earth-search.aws.element84.com/v0'
results = satsearch.Search.search(url=URL,
                                  collections=['sentinel-s2-l2a-cogs'],
                                  datetime=dates,
                                  bbox=bbox,
                                  sort=['-properties.datetime'])

# 18 items found
items = results.items()
print(len(items))
items.save('single-file-stac.json')

# validation returns empty list
import json
from pystac.validation import validate_dict
with open('single-file-stac.json') as f:
    js = json.load(f)
print(validate_dict(js))

cat = pystac.read_file('single-file-stac.json')
```
