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
                    Optional)

from new_couchbase.impl import BucketFactory
from new_couchbase.api.bucket import BucketInterface

from new_couchbase.result import PingResult
from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.scope import ScopeInterface
from new_couchbase.api import ApiImplementation


if TYPE_CHECKING:
    from new_couchbase.api.cluster import ClusterInterface

class Bucket(BucketInterface):

    def __init__(self,
                 cluster,  # type: ClusterInterface
                 bucket_name  # type: str
                 ):
        self._impl = BucketFactory.create_bucket(cluster, bucket_name)

    @property
    def api_implementation(self) -> ApiImplementation:
        return self._impl.api_implementation

    @property
    def connected(self) -> bool:
        return self._impl.connected

    @property
    def default_serializer(self) -> SerializerInterface:
        return self._impl.default_serializer

    @property
    def default_transcoder(self) -> TranscoderInterface:
        return self._impl.default_transcoder

    @property
    def name(self) -> str:
        return self._impl.name

    def collection(self, 
                collection_name # type: str
                ) -> CollectionInterface:
        return self._impl.collection(collection_name)

    def default_collection(self) -> CollectionInterface:
        return self._impl.default_collection()

    def default_scope(self) -> ScopeInterface:
        return self._impl.default_scope()

    def scope(self, 
                scope_name # type: str
                ) -> ScopeInterface:
        return self._impl.scope(scope_name)
