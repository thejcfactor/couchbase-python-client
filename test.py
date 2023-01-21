
<<<<<<< HEAD
from new_couchbase.cluster import Cluster
from new_couchbase.auth import PasswordAuthenticator
from new_couchbase.options import ClusterOptions
from new_couchbase.exceptions import DocumentNotFoundException

host = 'protostellar://10.66.166.137:18098'
# host = 'couchbase://localhost'
opts = ClusterOptions(PasswordAuthenticator('Administrator', 'password'))
cluster = Cluster(host, opts)

bucket = cluster.bucket('travel-sample')
default_coll = bucket.default_collection()
inventory_scope = bucket.scope('inventory')
airline_coll = inventory_scope.collection('airline')

res = airline_coll.get('airline_10')
print(f'{res.content_as[dict]}\n')

res = default_coll.upsert('test-key-1', {'what': 'lets hope this works'})
print(f'{res}\n')

res = default_coll.get('test-key-1')
print(f'{res}\n')

default_coll.remove('test-key-1')
try:
    res = default_coll.get('test-key-1')
except DocumentNotFoundException as ex:
    pass

keyspace = '`travel-sample`'
q_str = f'SELECT * FROM {keyspace} LIMIT 5;'

q_res = cluster.query(q_str)
for row in q_res.rows():
    print(f'Found row: {row}')

print(f'Query Metadata: {q_res.metadata()}\n')


# keyspace = '`travel-sample`.`inventory`.`airline`'
# # keyspace = 'airline'
# q_str = f'SELECT * FROM {keyspace} LIMIT 5;'

# q_res = inventory_scope.query(q_str)
# for row in q_res.rows():
#     print(f'Found row: {row}')

# print(f'Query Metadata: {q_res.metadata()}')
=======
# from new_couchbase.cluster import Cluster
# from new_couchbase.auth import PasswordAuthenticator
# from new_couchbase.options import ClusterOptions
# # from couchbase.cluster import Cluster
# # from couchbase.auth import PasswordAuthenticator
# # from couchbase.options import ClusterOptions

# opts = ClusterOptions(PasswordAuthenticator('Administrator', 'password'))

# cluster = Cluster('protostellar://10.66.166.137:18098', opts)
# # cluster = Cluster('couchbase://localhost', opts)

# bucket = cluster.bucket('beer-sample')
# coll = bucket.default_collection()

# # coll = bucket.collection('test')

# res = coll.get('21st_amendment_brewery_cafe')
# print(res.content_as[dict])

# res = coll.upsert('test-key-1', {'what': 'lets hope this works'})
# print(res)

from couchbase.tests.query_t import QueryParamTests

method_list = [meth for meth in dir(QueryParamTests) if callable(getattr(QueryParamTests, meth)) and not meth.startswith('__') and meth.startswith('test')]
print('\n'.join(map(lambda m: f"'{m}',", sorted(method_list))))
>>>>>>> 478f15c (rebase on master + more updates)
