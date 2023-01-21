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

from __future__ import annotations

from typing import Any, Dict, Union, TYPE_CHECKING

# from new_couchbase.api.cluster import ClusterInterface
# from new_couchbase.api.bucket import BucketInterface
# from new_couchbase.api.scope import ScopeInterface
# from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api import ApiImplementation


if TYPE_CHECKING:
    from new_couchbase.options import ClusterOptions
    from new_couchbase.classic.bucket import Bucket as ClassicBucket
    from new_couchbase.classic.cluster import Cluster as ClassicCluster
    from new_couchbase.classic.scope import Scope as ClassicScope
    from new_couchbase.protostellar.bucket import Bucket as ProtostellarBucket
    from new_couchbase.protostellar.cluster import Cluster as ProtostellarCluster
    from new_couchbase.protostellar.scope import Scope as ProtostellarScope

class ClusterFactory:

    @staticmethod
    def create_cluster(connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> Any:

        if connstr.startswith('protostellar://'):
            from new_couchbase.protostellar.cluster import Cluster
            return Cluster(connstr, *options, **kwargs)
        else:
            from new_couchbase.classic.core.cluster import ClusterCore
            from new_couchbase.classic.cluster import Cluster
            return Cluster(ClusterCore(connstr, *options, **kwargs))

class BucketFactory:
    @staticmethod
    def create_bucket(cluster,  # type: Union[ClassicCluster, ProtostellarCluster]
                 name, # type: str
                 ) -> Any:
        
        if cluster.api_implementation == ApiImplementation.PROTOSTELLAR:
            from new_couchbase.protostellar.bucket import Bucket
            return Bucket(cluster, name)
        else:
            from new_couchbase.classic.bucket import Bucket
            from new_couchbase.classic.core.bucket import BucketCore
            return Bucket(BucketCore(cluster, name))


class ScopeFactory:
    @staticmethod
    def create_scope(bucket,  # type: Union[ClassicBucket, ProtostellarBucket]
                 scope_name, # type: str
                 ) -> Any:

        if bucket.api_implementation == ApiImplementation.PROTOSTELLAR:
            from new_couchbase.protostellar.scope import Scope
            return Scope(bucket, scope_name)
        else:
            from new_couchbase.classic.scope import Scope
            return Scope(bucket, scope_name)

class CollectionFactory:
    @staticmethod
    def create_collection(scope,  # type: Union[ClassicScope, ProtostellarScope]
                 collection_name, # type: str
                 ) -> Any:

        if scope.api_implementation == ApiImplementation.PROTOSTELLAR:
            from new_couchbase.protostellar.collection import Collection
            return Collection(scope, collection_name)
        else:
            from new_couchbase.classic.core.collection import CollectionCore
            from new_couchbase.classic.collection import Collection
            return Collection(CollectionCore(scope, collection_name))