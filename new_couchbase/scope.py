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

from new_couchbase.api import ApiImplementation
from new_couchbase.collection import Collection
from new_couchbase.n1ql import N1QLRequest
from new_couchbase.result import QueryResult


if TYPE_CHECKING:
    from new_couchbase.classic.bucket import Bucket as ClassicBucket
    from new_couchbase.protostellar.bucket import Bucket as ProtostellarBucket
    from new_couchbase.options import QueryOptions

class Scope:
    def __init__(self, bucket, # type: Union[ClassicBucket, ProtostellarBucket]
            scope_name # type: str
            ):
        if bucket.api_implementation == ApiImplementation.PROTOSTELLAR:
            from new_couchbase.protostellar.scope import Scope
            self._impl = Scope(bucket, scope_name)
        else:
            from new_couchbase.classic.scope import Scope
            self._impl = Scope(bucket, scope_name)

    @property
    def api_implementation(self) -> ApiImplementation:
        return self._impl.api_implementation

    @property
    def bucket_name(self) -> str:
        return self._impl.bucket_name

    @property
    def name(self) -> str:
        return self._impl.name

    def collection(self, 
                collection_name # type: str
                ) -> Collection:
        return self._impl.collection(collection_name)

    def query(
        self,
        statement,  # type: str
        *opts,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> QueryResult:
        n1ql_request = N1QLRequest(self._impl.query(statement, *opts, **kwargs))
        return QueryResult(n1ql_request)

    @staticmethod
    def default_name():
        return "_default"