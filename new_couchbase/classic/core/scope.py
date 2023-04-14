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


from typing import Any, Dict, Optional, TYPE_CHECKING


from new_couchbase.serializer import Serializer
from new_couchbase.transcoder import Transcoder
from new_couchbase.api import ApiImplementation
from new_couchbase.collection import Collection

from new_couchbase.classic.core.n1ql import N1QLQuery
from new_couchbase.classic.n1ql import N1QLRequest

from new_couchbase.classic.result import QueryResult
from new_couchbase.options import QueryOptions

if TYPE_CHECKING:
    from new_couchbase.classic.bucket import Bucket

class ScopeCore:
    def __init__(self, 
                bucket, # type: Bucket
                scope_name=None # type: Optional[str]
                ):
        self._bucket = bucket
        self._scope_name = ScopeCore.default_name() if scope_name is None else scope_name

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.CLASSIC

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
    def default_serializer(self) -> Serializer:
        return self._bucket.default_serializer

    @property
    def default_transcoder(self) -> Transcoder:
        return self._bucket.default_transcoder

    @property
    def name(self) -> str:
        return self._scope_name

    def collection(self, 
                collection_name # type: str
                ) -> Collection:
        return Collection(self, collection_name)
    
    def query(
        self,
        statement,  # type: str
        *opts,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> QueryResult:

        opt = QueryOptions()
        opts = list(opts)
        for o in opts:
            if isinstance(o, QueryOptions):
                opt = o
                opts.remove(o)
        # set the query context as this bucket and scope if not provided
        if not ('query_context' in opt or 'query_context' in kwargs):
            kwargs['query_context'] = '`{}`.`{}`'.format(self.bucket_name, self.name)

        query = N1QLQuery.create_query_object(
            statement, opt, **kwargs)
        return QueryResult(N1QLRequest.generate_n1ql_request(self.connection,
                                                             query.params))

    @staticmethod
    def default_name():
        return "_default"