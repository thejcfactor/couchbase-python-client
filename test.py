from datetime import datetime, timedelta


from new_couchbase.cluster import Cluster
from new_couchbase.auth import PasswordAuthenticator
from new_couchbase.options import ClusterOptions, GetOptions, UpsertOptions
from new_couchbase.exceptions import DocumentNotFoundException
from new_couchbase.management.buckets import BucketType, CreateBucketSettings

host = 'couchbase://localhost'
# host = 'couchbase://172.23.111.128'
opts = ClusterOptions(PasswordAuthenticator('Administrator', 'password'))
cluster = Cluster(host, opts)

bucket = cluster.bucket('beer-sample')
default_coll = bucket.default_collection()

# key = 'test-key-1'
# content = {'what': 'lets hope this works'}
# print(f'UTC now: {datetime.utcnow()}')
# res = default_coll.upsert(key, content, UpsertOptions(expiry=timedelta(seconds=30)))
# print(f'Upsert CAS: {res}\n')

# res = default_coll.get(key, GetOptions(with_expiry=True))
# print(f'{key} expiry: {res.expiry_time}\n')
# print(f'content: {res.content_as[dict]}')

res = default_coll.get('21st_amendment_brewery_cafe')
print(f'content: {res.content_as[dict]}')

# default_coll.remove(key)
# try:
#     res = default_coll.get(key)
#     print(res.content_as[dict])
# except DocumentNotFoundException as ex:
#     pass

# bm = cluster.buckets()
# bm.create_bucket(CreateBucketSettings(name='test-bucket',
#                                       bucket_type=BucketType.COUCHBASE,
#                                       ram_quota_mb=100,
#                                       num_replicas=1))
# bm.get_all_buckets()

from couchbase import get_metadata
print(get_metadata())