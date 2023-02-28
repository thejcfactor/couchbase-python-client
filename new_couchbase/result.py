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

import json
from datetime import datetime
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional)

from new_couchbase.api.result import (ClusterInfoResultInterface,
                                      CounterResultInterface,
                                    DiagnosticsResultInterface,
                                    ExistsResultInterface,
                                    GetReplicaResultInterface,
                                    GetResultInterface,
                                    LookupInResultInterface,
                                    MutateInResultInterface,
                                    MutationResultInterface,
                                    PingResultInterface,
                                    QueryResultInterface)
from new_couchbase.diagnostics import (ClusterState,
                                              EndpointDiagnosticsReport,
                                              EndpointPingReport,
                                              EndpointState,
                                              ServiceType)
from new_couchbase.mutation_state import MutationToken

if TYPE_CHECKING:
    from new_couchbase.n1ql import N1QLRequest

"""

Python SDK Diagnostic Operation Results

"""

class ClusterInfoResult(ClusterInfoResultInterface):
    def __init__(self, 
                core_result # type: ClusterInfoResultInterface
                ):
        self._core_result = core_result

    @property
    def is_community(self) -> Optional[bool]:
        return self._core_result.is_community

    @property
    def is_enterprise(self) -> Optional[bool]:
        return self._core_result.is_enterprise

    @property
    def nodes(self) -> Any:
        return self._core_result.nodes

    @property
    def server_version(self) -> Optional[str]:
        return self._core_result.server_version

    @property
    def server_version_build(self) -> Optional[int]:
        return self._core_result.server_version_build

    @property
    def server_version_full(self) -> Optional[str]:
        return self._core_result.server_version_full

    @property
    def server_version_short(self) -> Optional[float]:
        return self._core_result.server_version_short

    def __repr__(self) -> str:
        return self._core_result.__repr__()

class DiagnosticsResult(DiagnosticsResultInterface):
    
    def __init__(self, 
                core_result # type: DiagnosticsResultInterface
                ):
        self._core_result = core_result

    @property
    def endpoints(self) -> Dict[str, Any]:
        return self._core_result.endpoints

    @property
    def id(self) -> str:
        return self._core_result.id

    @property
    def sdk(self) -> str:
        return self._core_result.sdk

    @property
    def state(self) -> ClusterState:
        return self._core_result.state

    @property
    def success(self) -> bool:
        return self._core_result.success

    @property
    def version(self) -> int:
        return self._core_result.version

    def as_json(self) -> str:
        return self._core_result.as_json()

    def __repr__(self) -> str:
        return self._core_result.__repr__()

class PingResult(PingResultInterface):
    def __init__(self, 
                core_result # type: PingResultInterface
                ):
        self._core_result = core_result

    @property
    def endpoints(self) -> Dict[str, Any]:
        return self._core_result.endpoints

    @property
    def id(self) -> str:
        return self._core_result.id

    @property
    def sdk(self) -> str:
        return self._core_result.sdk

    @property
    def success(self) -> bool:
        return self._core_result.success

    @property
    def version(self) -> int:
        return self._core_result.version

    def as_json(self) -> str:
        return self._core_result.as_json()

    def __repr__(self) -> str:
        return self._core_result.__repr__()

"""

Python SDK Key-Value Operation Results

"""

class CounterResult(CounterResultInterface):
    def __init__(self, 
                core_result # type: CounterResultInterface
                ):
        self._core_result = core_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._core_result.cas

    @property
    def content(self) -> Optional[int]:
        """
            Optional[int]: The value of the document after the operation completed.
        """
        return self._core_result.content

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._core_result.flags

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._core_result.key

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self._core_result.success

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._core_result.value

    def __repr__(self):
        return self._core_result.__repr__()
    

class ExistsResult(ExistsResultInterface):
    def __init__(self, 
                core_result # type: ExistsResultInterface
                ):
        self._core_result = core_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._core_result.cas

    @property
    def exists(self) -> bool:
        """
            bool: True if the document exists, false otherwise.
        """
        return self._core_result.exists

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._core_result.flags

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._core_result.key

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self._core_result.success

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._core_result.value

    def __repr__(self):
        return self._core_result.__repr__()

class GetReplicaResult(GetReplicaResultInterface):
    def __init__(self, 
                core_result # type: GetReplicaResultInterface
                ):
        self._core_result = core_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._core_result.cas

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get(key)
                value = res.content_as[dict]

        """
        return self._core_result.content_as

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._core_result.flags

    @property
    def is_active(self) -> bool:
        """
        ** DEPRECATED ** use is_replica

            bool: True if the result is the active document, False otherwise.
        """
        return not self.is_replica

    @property
    def is_replica(self) -> bool:
        """
            bool: True if the result is a replica, False otherwise.
        """
        return self._core_result.is_replica

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._core_result.key

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self._core_result.success

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._core_result.value

    def __repr__(self):
        return self._core_result.__repr__()

class GetResult(GetResultInterface):
    def __init__(self, 
                core_result # type: GetResultInterface
                ):
        self._core_result = core_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._core_result.cas

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get(key)
                value = res.content_as[dict]

        """
        return self._core_result.content_as

    @property
    def expiry_time(self) -> Optional[datetime]:
        """
            Optional[datetime]: The expiry of the document, if it was requested.
        """
        return self._core_result.expiry_time

    @property
    def expiryTime(self) -> Optional[datetime]:
        """
        ** DEPRECATED ** use expiry_time

        Optional[datetime]: The expiry of the document, if it was requested.
        """
        if hasattr(self._core_result, 'expiryTime'):
            return self._core_result.expiryTime
        return None

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._core_result.flags

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._core_result.key

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self._core_result.success

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._core_result.value

    def __repr__(self):
        return self._core_result.__repr__()


class MutationResult(MutationResultInterface):
    def __init__(self, 
                core_result # type: MutationResultInterface
                ):
        self._core_result = core_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._core_result.cas

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._core_result.flags

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._core_result.key

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self._core_result.success

    def mutation_token(self) -> Optional[MutationToken]:
        """Get the operation's mutation token, if it exists.

        Returns:
            Optional[:class:`.MutationToken`]: The operation's mutation token.
        """
        return self._core_result.mutation_token()

    def __repr__(self):
        return self._core_result.__repr__()

# Sub-document Operations

class LookupInResult(LookupInResultInterface):
    def __init__(self, 
                core_result # type: LookupInResultInterface
                ):
        self._core_result = core_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._core_result.cas

    @property
    def content_as(self) -> Any:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a dict::

                res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
                value = res.content_as[dict](0)
        """
        return self._core_result.content_as

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._core_result.flags

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._core_result.key

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self._core_result.success

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._core_result.value

    def exists(self,
              index  # type: int
            ) -> bool:
        """Check if the subdocument path exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the provided index is out of range.

        Returns:
            bool: True if the path exists.  False if the path does not exist.
        """
        return self._core_result.exists(index)

    def __repr__(self):
        return self._core_result.__repr__()

class MutateInResult(MutateInResultInterface):
    def __init__(self, 
                core_result # type: MutateInResultInterface
                ):
        self._core_result = core_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._core_result.cas

    @property
    def content_as(self) -> Any:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a str::

                res = collection.mutate_in(key, (SD.upsert("city", "New City"),
                                                SD.replace("faa", "CTY")))
                value = res.content_as[str](0)
        """
        return self._core_result.content_as


    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._core_result.flags

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._core_result.key

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self._core_result.success

    def mutation_token(self) -> Optional[MutationToken]:
        """Get the operation's mutation token, if it exists.

        Returns:
            Optional[:class:`.MutationToken`]: The operation's mutation token.
        """
        return self._core_result.mutation_token()

    def __repr__(self):
        return self._core_result.__repr__()

"""

Python SDK Streaming Results

"""

class QueryResult(QueryResultInterface):
    def __init__(self,
                core_result # type: QueryResultInterface
        ):
        self._core_result = core_result

    def rows(self):
        """The rows which have been returned by the query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        return self._core_result.__iter__()

    def execute(self):
        """Convenience method to execute the query.

        Returns:
            List[Any]:  A list of query results.

        Example:
            q_rows = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;').execute()

        """
        return self._core_result.execute()

    def metadata(self):
        """The meta-data which has been returned by the query.

        Returns:
            :class:`~couchbase.n1ql.QueryMetaData`: An instance of :class:`~couchbase.n1ql.QueryMetaData`.
        """
        return self._core_result.metadata()

    def __iter__(self):
        return self._core_result.__iter__()

    def __aiter__(self):
        return self._core_result.__aiter__()

    def __repr__(self):
        return self._core_result.__repr__()