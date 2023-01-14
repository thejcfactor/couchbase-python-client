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

from typing import Any, Dict, Optional, List, Tuple, TYPE_CHECKING

from new_couchbase.api import ApiImplementation
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.scope import ScopeInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.api.collection import CollectionInterface
from new_couchbase.collection import Collection


if TYPE_CHECKING:
    from new_couchbase.api.bucket import BucketInterface


class Scope(ScopeInterface):
    def __init__(self, 
                bucket, # type: BucketInterface
                scope_name # type: str
                ):
        self._bucket = bucket
        self._scope_name = scope_name
        # self._query_service = query.QueryStub(self.channel)

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.PROTOSTELLAR

    @property
    def bucket_name(self) -> str:
        return self._bucket.name

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._bucket.connection

    @property
    def default_serializer(self) -> SerializerInterface:
        return self._bucket.default_serializer

    @property
    def default_transcoder(self) -> TranscoderInterface:
        return self._bucket.default_transcoder

    @property
    def metadata(self) -> List[Tuple[str]]:
        """
        **INTERNAL**
        """
        return self._bucket.metadata

    @property
    def name(self) -> str:
        return self._scope_name

    def collection(self, 
                collection_name # type: str
                ) -> CollectionInterface:
        return Collection(self, collection_name)

    @staticmethod
    def default_name():
        return "_default"