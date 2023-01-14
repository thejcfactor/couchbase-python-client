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

from typing import Optional, List, Tuple, TYPE_CHECKING

# from couchbase.serializer import Serializer
# from protostellar.collection import Collection
# from protostellar.scope import Scope
# from protostellar.transcoder import Transcoder

from new_couchbase.api.bucket import BucketInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api.scope import ScopeInterface
from new_couchbase.api import ApiImplementation

from new_couchbase.scope import Scope
from new_couchbase.collection import Collection

if TYPE_CHECKING:
    from new_couchbase.api.cluster import ClusterInterface


class Bucket(BucketInterface):
    def __init__(self, 
                cluster, # type: ClusterInterface
                bucket_name # type: str
                ):
        self._cluster = cluster
        self._bucket_name = bucket_name

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.PROTOSTELLAR

    @property
    def connected(self) -> bool:
        return self._cluster.connected

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._cluster.connection

    @property
    def default_serializer(self) -> SerializerInterface:
        return self._cluster.default_serializer

    @property
    def default_transcoder(self) -> TranscoderInterface:
        return self._cluster.default_transcoder

    @property
    def metadata(self) -> List[Tuple[str]]:
        """
        **INTERNAL**
        """
        return self._cluster.metadata

    @property
    def name(self) -> str:
        return self._bucket_name

    def collection(self, collection_name  # type: str
                   ) -> CollectionInterface:
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self) -> CollectionInterface:
        scope = self.default_scope()
        return scope.collection(Collection.default_name())

    def default_scope(self) -> ScopeInterface:
        return self.scope(Scope.default_name())

    def scope(self, name,  # type: str
              ) -> ScopeInterface:
        return Scope(self, name)

