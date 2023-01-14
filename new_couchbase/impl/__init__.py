#  Copyright 2016-2023. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from typing import Any, Dict, TYPE_CHECKING

from new_couchbase.api.cluster import ClusterInterface
from new_couchbase.api.bucket import BucketInterface
from new_couchbase.api.scope import ScopeInterface
from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api import ApiImplementation


if TYPE_CHECKING:
    from new_couchbase.options import ClusterOptions

class ClusterFactory:

    @staticmethod
    def create_cluster(connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> ClusterInterface:

        from new_couchbase.impl.protostellar.cluster import Cluster as ProtostellarCluster
        from new_couchbase.impl.server.core.cluster import ClusterCore as ServerClusterCore
        from new_couchbase.impl.server.cluster import Cluster as ServerCluster
        

        if connstr.startswith('protostellar://'):
            return ProtostellarCluster(connstr, *options, **kwargs)
        else:
            return ServerCluster(ServerClusterCore(connstr, *options, **kwargs))

class BucketFactory:
    @staticmethod
    def create_bucket(cluster,  # type: ClusterInterface
                 name, # type: str
                 ) -> BucketInterface:
        
        from new_couchbase.impl.protostellar.bucket import Bucket as ProtostellarBucket
        from new_couchbase.impl.server.bucket import Bucket as ServerBucket
        from new_couchbase.impl.server.core.bucket import BucketCore as ServerBucketCore

        if cluster.api_implementation == ApiImplementation.PROTOSTELLAR:
            return ProtostellarBucket(cluster, name)
        else:
            return ServerBucket(ServerBucketCore(cluster, name))


class ScopeFactory:
    @staticmethod
    def create_scope(bucket,  # type: BucketInterface
                 scope_name, # type: str
                 ) -> ScopeInterface:
        
        from new_couchbase.impl.protostellar.scope import Scope as ProtostellarScope
        from new_couchbase.impl.server.scope import Scope as ServerScope

        if bucket.api_implementation == ApiImplementation.PROTOSTELLAR:
            return ProtostellarScope(bucket, scope_name)
        else:
            return ServerScope(bucket, scope_name)

class CollectionFactory:
    @staticmethod
    def create_collection(scope,  # type: ScopeInterface
                 collection_name, # type: str
                 ) -> CollectionInterface:

        from new_couchbase.impl.protostellar.collection import Collection as ProtostellarCollection
        from new_couchbase.impl.server.core.collection import CollectionCore as ServerCollectionCore
        from new_couchbase.impl.server.collection import Collection as ServerCollection

        if scope.api_implementation == ApiImplementation.PROTOSTELLAR:
            return ProtostellarCollection(scope, collection_name)
        else:
            return ServerCollection(ServerCollectionCore(scope, collection_name))