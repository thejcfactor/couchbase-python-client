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

from new_couchbase.impl import ScopeFactory
from new_couchbase.api.scope import ScopeInterface

from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.scope import ScopeInterface
from new_couchbase.api import ApiImplementation


if TYPE_CHECKING:
    from new_couchbase.api.bucket import BucketInterface

class Scope(ScopeInterface):
    def __init__(self, bucket, # type: BucketInterface
            scope_name # type: str
            ):
        self._impl = ScopeFactory.create_scope(bucket, scope_name)

    @property
    def api_implementation(self) -> ApiImplementation:
        return self._impl.api_implementation

    @property
    def bucket_name(self) -> str:
        return self._impl.bucket_name

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

    @staticmethod
    def default_name():
        return "_default"