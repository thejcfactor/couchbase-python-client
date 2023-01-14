import importlib
import importlib.util
import pathlib
import os


# kv
kv_grpc_spec = importlib.util.spec_from_file_location('kv.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'kv.v1_pb2_grpc.py'))
kv_grpc_module = importlib.util.module_from_spec(kv_grpc_spec)
kv_grpc_spec.loader.exec_module(kv_grpc_module)

# view
view_grpc_spec = importlib.util.spec_from_file_location('view.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'view.v1_pb2_grpc.py'))
view_grpc_module = importlib.util.module_from_spec(view_grpc_spec)
view_grpc_spec.loader.exec_module(view_grpc_module)

# routing
routing_grpc_spec = importlib.util.spec_from_file_location('routing.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'routing.v1_pb2_grpc.py'))
routing_grpc_module = importlib.util.module_from_spec(routing_grpc_spec)
routing_grpc_spec.loader.exec_module(routing_grpc_module)

# search
search_grpc_spec = importlib.util.spec_from_file_location('search.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'search.v1_pb2_grpc.py'))
search_grpc_module = importlib.util.module_from_spec(search_grpc_spec)
search_grpc_spec.loader.exec_module(search_grpc_module)

# transactions
transactions_grpc_spec = importlib.util.spec_from_file_location('transactions.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'transactions.v1_pb2_grpc.py'))
transactions_grpc_module = importlib.util.module_from_spec(transactions_grpc_spec)
transactions_grpc_spec.loader.exec_module(transactions_grpc_module)

# query
query_grpc_spec = importlib.util.spec_from_file_location('query.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'query.v1_pb2_grpc.py'))
query_grpc_module = importlib.util.module_from_spec(query_grpc_spec)
query_grpc_spec.loader.exec_module(query_grpc_module)

# analytics
analytics_grpc_spec = importlib.util.spec_from_file_location('analytics.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'analytics.v1_pb2_grpc.py'))
analytics_grpc_module = importlib.util.module_from_spec(analytics_grpc_spec)
analytics_grpc_spec.loader.exec_module(analytics_grpc_module)

# collection_admin
collection_admin_grpc_spec = importlib.util.spec_from_file_location('collection.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'admin', 'collection.v1_pb2_grpc.py'))
collection_admin_grpc_module = importlib.util.module_from_spec(collection_admin_grpc_spec)
collection_admin_grpc_spec.loader.exec_module(collection_admin_grpc_module)

# bucket_admin
bucket_admin_grpc_spec = importlib.util.spec_from_file_location('bucket.v1_pb2_grpc', os.path.join(pathlib.Path(__file__).parent, 'proto', 'couchbase', 'admin', 'bucket.v1_pb2_grpc.py'))
bucket_admin_grpc_module = importlib.util.module_from_spec(bucket_admin_grpc_spec)
bucket_admin_grpc_spec.loader.exec_module(bucket_admin_grpc_module)
