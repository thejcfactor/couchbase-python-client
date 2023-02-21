from protostellar.cluster import Cluster
from protostellar.options import ConnectOptions


cluster = Cluster('10.66.166.137:18098', ConnectOptions(username='Administrator', password='password'))

bucket = cluster.bucket('beer-sample')
scope = bucket.default_scope()
collection = bucket.default_collection()

res = collection.upsert('test_key', {'what': 'sn-test-doc'})
print(res)
res = collection.get('test_key')
print(res)
# res = collection.exists('test_key')
# print(res)

# bm = cluster.buckets()

# bm.get_all_buckets()

# query_str = 'SELECT * FROM `beer-sample` LIMIT 2;'
# query_str = 'NOT N1QL!!!'
# res = scope.query(query_str)

# for r in res.rows():
#     print(f'Found row: {r}')

# print(res.metadata())
