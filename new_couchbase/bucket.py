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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional,
                    Union)

from new_couchbase.result import PingResult
from new_couchbase.collection import Collection
from new_couchbase.scope import Scope
from new_couchbase.api import ApiImplementation


if TYPE_CHECKING:
    from new_couchbase.classic.cluster import Cluster as ClassicCluster
    from new_couchbase.protostellar.cluster import Cluster as ProtostellarCluster

class Bucket:

    def __init__(self,
                 cluster,  # type: Union[ClassicCluster, ProtostellarCluster]
                 bucket_name  # type: str
                 ):
        if cluster.api_implementation == ApiImplementation.PROTOSTELLAR:
            from new_couchbase.protostellar.bucket import Bucket
            self._impl = Bucket(cluster, bucket_name)
        else:
            from new_couchbase.classic.bucket import Bucket
            self._impl = Bucket(cluster, bucket_name)

    @property
    def api_implementation(self) -> ApiImplementation:
        return self._impl.api_implementation

    @property
    def connected(self) -> bool:
        return self._impl.connected

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._impl.connection

    @property
    def name(self) -> str:
        return self._impl.name

    def collection(self, 
                   collection_name # type: str
                   ) -> Collection:
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self) -> Collection:
        scope = self.default_scope()
        return scope.collection()
        # return self._impl.default_collection()

    def default_scope(self) -> Scope:
        return Scope(self._impl)
        # return self._impl.default_scope()

    def scope(self, 
              scope_name # type: str
              ) -> Scope:
        return Scope(self._impl, scope_name)
