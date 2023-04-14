import sys
import new_couchbase.constants as c
from new_couchbase.transcoder import COMMON2UNIFIED, LEGACY2UNIFIED

import new_couchbase.exceptions as e

# print(f'{c.FMT_BYTES=}')
# print(f'{c.FMT_COMMON_MASK=}')
# print(f'{c.FMT_JSON=}')
# print(f'{c.FMT_LEGACY_MASK=}')
# print(f'{c.FMT_PICKLE=}')
# print(f'{c.FMT_UTF8=}')

# print(f'{COMMON2UNIFIED=}')

# print(f'{LEGACY2UNIFIED=}')

couchbase_exceptions = []
for ex in dir(e):
    exp = getattr(sys.modules['new_couchbase.exceptions'], ex)
    try:
        if issubclass(exp, e.CouchbaseException):
            couchbase_exceptions.append(exp)
    except TypeError:
        pass
for ex in couchbase_exceptions:
    print(ex.__name__)
#print(f'{couchbase_exceptions}')