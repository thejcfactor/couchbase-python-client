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

import json
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional,
                    Union)

from couchbase.pycbc_core import (binary_operation,
                                  kv_operation,
                                  operations,
                                  subdoc_operation)

from new_couchbase.exceptions import InvalidArgumentException
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.result import GetResult, MutationResult
from new_couchbase.common._utils import timedelta_as_microseconds

if TYPE_CHECKING:
    from new_couchbase.api.scope import ScopeInterface
    from new_couchbase.common._utils import JSONType

class CollectionCore:
    """
    **INTERNAL**
    Not a part of the public API.
    """
    def __init__(self, 
                scope, # type: ScopeInterface
                name # type str
                ):
        if not scope:
            raise InvalidArgumentException(message="Collection must be given a scope")
        # if not scope.connection:
        #     raise RuntimeError("No connection provided")
        self._scope = scope
        self._collection_name = name
        self._connection = scope.connection

    @property
    def default_transcoder(self) -> TranscoderInterface:
        """
        **INTERNAL**
        """
        return self._scope.default_transcoder

    @property
    def name(self) -> str:
        """
        **INTERNAL**
        """
        return self._collection_name

    def _get_connection_args(self) -> Dict[str, Any]:
        """
        **INTERNAL**
        """
        return {
            "conn": self._connection,
            "bucket": self._scope.bucket_name,
            "scope": self._scope.name,
            "collection_name": self.name
        }

    def _parse_durability_timeout(self,
                              **kwargs  # type: Dict[str, Any]
                              ) -> Dict[str, Any]:
        """**INTERNAL**
        Parses the mutaiton operation options.  If synchronous durability has been set and no timeout provided, the
        default timeout will be set to the default KV durable timeout (10 seconds).
        """
        if 'durability' in kwargs and isinstance(kwargs['durability'], int) and 'timeout' not in kwargs:
            kwargs['timeout'] = timedelta_as_microseconds(timedelta(seconds=10))

        return kwargs

    # this is utilized by the async collection
    def set_connection(self):
        """
        **INTERNAL**
        """
        self._connection = self._scope.connection

    def get(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> GetResult:
        """
        **INTERNAL**
        """
        # projections = kwargs.get("project")
        # if isinstance(projections, list) and len(projections) > 16:
        #     raise InvalidArgumentException(
        #         f"Maximum of 16 projects allowed. Provided {len(projections)}"
        #     )
        op_type = operations.GET.value
        return kv_operation(**self._get_connection_args(),
                            key=key,
                            op_type=op_type,
                            op_args=kwargs)

    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        """
        **INTERNAL**
        """
        final_args = self._parse_durability_timeout(**kwargs)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        transcoded_value = transcoder.encode_value(value)

        op_type = operations.UPSERT.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            value=transcoded_value,
            op_type=op_type,
            op_args=final_args
        )