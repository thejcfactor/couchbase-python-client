
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