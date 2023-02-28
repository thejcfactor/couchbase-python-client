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
from new_couchbase.exceptions import (DocumentNotFoundException,
                                        InvalidIndexException,
                                        PathExistsException,
                                        PathMismatchException,
                                        PathNotFoundException,
                                        SubdocCantInsertValueException)
from new_couchbase.diagnostics import (ClusterState,
                                              EndpointDiagnosticsReport,
                                              EndpointPingReport,
                                              EndpointState,
                                              ServiceType)
from new_couchbase.mutation_state import MutationToken
from new_couchbase.subdocument import SubDocStatus

if TYPE_CHECKING:
    from couchbase.pycbc_core import result
    from new_couchbase.classic.n1ql import N1QLRequest

"""

Python SDK Diagnostic Operation Results

"""

class ClusterInfoResult(ClusterInfoResultInterface):
    def __init__(
        self,
        result  # type: Dict[str, Any]
    ):
        self._raw_result = result
        # version string should be X.Y.Z-XXXX-YYYY
        self._server_version_raw = None
        self._server_version = None
        self._server_version_short = None
        self._server_build = None
        self._is_enterprise = None

    @property
    def is_community(self) -> Optional[bool]:
        """
            bool: True if connected Couchbase Server is Community edition, false otherwise.
        """
        if not self._is_community:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split('-')
            if len(tokens) == 3:
                self._is_community = tokens[2].upper() == 'COMMUNITY'

        return self._is_community

    @property
    def is_enterprise(self) -> Optional[bool]:
        """
            bool: True if connected Couchbase Server is Enterprise edition, false otherwise.
        """
        if not self._is_enterprise:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split('-')
            if len(tokens) == 3:
                self._is_enterprise = tokens[2].upper() == 'ENTERPRISE'

        return self._is_enterprise

    @property
    def nodes(self):
        return self._raw_result.get('nodes', None)

    @property
    def server_version(self) -> Optional[str]:
        if not self._server_version:
            self._set_server_version()

        if self._server_version_raw:
            self._server_version = self._server_version_raw[:10]

        return self._server_version

    @property
    def server_version_build(self) -> Optional[int]:
        """
            Optional[int]: The build version of the connected Couchbase Server.
        """
        if not self._server_build:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split('-')
            if len(tokens) == 3:
                self._server_build = int(tokens[1])

        return self._server_build

    @property
    def server_version_full(self) -> Optional[str]:
        """
            Optional[str]: The full version details of the connected Couchbase Server.
        """
        if not self._server_version_raw:
            self._set_server_version()

        return self._server_version_raw

    @property
    def server_version_short(self) -> Optional[float]:
        """
            Optional[float]: The version of the connected Couchbase Server in Major.Minor form.
        """
        if not self._server_version_short:
            self._set_server_version()

        if self._server_version_raw:
            self._server_version_short = float(self._server_version_raw[:3])

        return self._server_version_short


    def _set_server_version(self):
        version = None
        for n in self.nodes:
            v = n['version']
            if version is None:
                version = v
            elif v != version:
                # mixed versions -- not supported
                version = None
                break

        self._server_version_raw = version

    def __repr__(self):
        return 'ClusterInfoResult:{}'.format(self._raw_result)

class DiagnosticsResult(DiagnosticsResultInterface):
    def __init__(self,
                result # type: Dict[str, Any]
            ):
        self._raw_result = result
        svc_endpoints = self._raw_result.get('endpoints', None)
        self._endpoints = {}
        if svc_endpoints:
            for service, endpoints in svc_endpoints.items():
                service_type = ServiceType(service)
                self._endpoints[service_type] = []
                for endpoint in endpoints:
                    self._endpoints[service_type].append(
                        EndpointDiagnosticsReport(service_type, endpoint))

    @property
    def endpoints(self) -> Dict[str, Any]:
        """
            Dict[str, Any]: A map of service endpoints and their diagnostic status.
        """
        return self._endpoints
    
    @property
    def id(self) -> str:
        """
            str: The unique identifier for this report.
        """
        return self._raw_result.get('id', None)

    @property
    def sdk(self) -> str:
        """
            str: The name of the SDK which generated this report.
        """
        return self._raw_result.get('sdk', None)

    @property
    def state(self) -> ClusterState:
        """
            :class:`~couchbase.diagnostics.ClusterState`: The cluster state.
        """
        num_found = 0
        num_connected = 0
        for endpoints in self._endpoints.values():
            for endpoint in endpoints:
                num_found += 1
                if endpoint.state == EndpointState.Connected:
                    num_connected += 1

        if num_found == num_connected:
            return ClusterState.Online
        if num_connected > 0:
            return ClusterState.Degraded
        return ClusterState.Offline

    @property
    def version(self) -> int:
        """
            int: The version number of this report.
        """
        return self._raw_result.get('version', None)


    def as_json(self) -> str:
        """Returns a JSON formatted diagnostics report.

        Returns:
            str: JSON formatted diagnostics report.
        """
        return_val = {
            'version': self.version,
            'id': self.id,
            'sdk': self.sdk,
            'services': {k.value: list(map(lambda epr: epr.as_dict(), v)) for k, v in self.endpoints.items()}
        }

        return json.dumps(return_val)

    def __repr__(self):
        return 'DiagnosticsResult:{}'.format(self._raw_result)


class PingResult(PingResultInterface):
    def __init__(self, result # type: Dict[str, Any]
        ):
        self._raw_result = result
        svc_endpoints = self._raw_result.get('endpoints', None)
        self._endpoints = {}
        if svc_endpoints:
            for service, endpoints in svc_endpoints.items():
                service_type = ServiceType(service)
                self._endpoints[service_type] = []
                for endpoint in endpoints:
                    self._endpoints[service_type].append(
                        EndpointPingReport(service_type, endpoint))

    @property
    def endpoints(self) -> Dict[str, Any]:
        """
            Dict[str, Any]: A map of service endpoints and their ping status.
        """
        return self._endpoints

    @property
    def id(self) -> str:
        """
            str: The unique identifier for this report.
        """
        return self._raw_result.get('id', None)

    @property
    def sdk(self) -> str:
        """
            str: The name of the SDK which generated this report.
        """
        return self._raw_result.get('sdk', None)

    @property
    def success(self) -> bool:
        return self._raw_result is not None and len(self._raw_result) > 0

    @property
    def version(self) -> int:
        """
            int: The version number of this report.
        """
        return self._raw_result.get('version', None)

    def as_json(self) -> str:
        """Returns a JSON formatted diagnostics report.

        Returns:
            str: JSON formatted diagnostics report.
        """
        return_val = {
            'version': self.version,
            'id': self.id,
            'sdk': self.sdk,
            'services': {k.value: list(map(lambda epr: epr.as_dict(), v)) for k, v in self.endpoints.items()}
        }

        return json.dumps(return_val)

    def __repr__(self):
        return 'PingResult:{}'.format(self._raw_result)

"""

Python SDK Key-Value Operation Results

"""

# Binary Operations

class CounterResult(CounterResultInterface):

    def __init__(self,
                 orig # type: result
            ):
        self._orig = orig

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get('cas', None)

    @property
    def content(self) -> Optional[int]:
        """
            Optional[int]: The value of the document after the operation completed.
        """
        return self._orig.raw_result.get('content', None)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get('key', None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._orig.raw_result.get('value', None)

    def __repr__(self):
        return 'CounterResult:{}'.format(self._orig)

# Multi Operations

# Standard Operations


class ContentProxy:
    """
    Used to provide access to Result content via Result.content_as[type]
    """

    def __init__(self, content):
        self._content = content

    def __getitem__(self,
                    type_       # type: Any
                    ) -> Any:
        """

        :param type_: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return type_(self._content)

class ExistsResult(ExistsResultInterface):
    def __init__(self,
                 orig # type: result
            ):
        self._orig = orig

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get('cas', None)

    @property
    def exists(self) -> bool:
        """
            bool: True if the document exists, false otherwise.
        """
        return self._orig.raw_result.get('exists', False)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get('key', None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._orig.raw_result.get('value', None)

    def __repr__(self):
        return 'ExistsResult:{}'.format(self._orig)

class GetReplicaResult(GetReplicaResultInterface):
    def __init__(self, orig # type: result
        ):
        self._orig = orig

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get('cas', None)

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get(key)
                value = res.content_as[dict]

        """
        return ContentProxy(self.value)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get('flags', None)

    @property
    def is_active(self) -> bool:
        """
        ** DEPRECATED ** use is_replica

        bool: True if the result is the active document, False otherwise.
        """
        return not self._orig.raw_result.get('is_replica')

    @property
    def is_replica(self) -> bool:
        """
            bool: True if the result is a replica, False otherwise.
        """
        return self._orig.raw_result.get('is_replica')

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get('key', None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._orig.raw_result.get('value', None)

    def __repr__(self):
        return 'GetReplicaResult:{}'.format(self._orig)

class GetResult(GetResultInterface):
    def __init__(self, orig # type: result
        ):
        self._orig = orig

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get('cas', None)

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get(key)
                value = res.content_as[dict]

        """
        return ContentProxy(self.value)

    @property
    def expiry_time(self) -> Optional[datetime]:
        """
            Optional[datetime]: The expiry of the document, if it was requested.
        """
        time_ms = self._orig.raw_result.get('expiry', None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def expiryTime(self) -> Optional[datetime]:
        """
        ** DEPRECATED ** use expiry_time

        Optional[datetime]: The expiry of the document, if it was requested.
        """
        # make this a datetime!
        time_ms = self._orig.raw_result.get('expiry', None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get('key', None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._orig.raw_result.get('value', None)

    def __repr__(self):
        return 'GetResult:{}'.format(self._orig)

class MutationResult(MutationResultInterface):
    def __init__(self, 
                orig # type: result
                ):
        self._orig = orig
        self._raw_mutation_token = self._orig.raw_result.get('mutation_token', None)
        self._mutation_token = None

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get('cas', None)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get('key', None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    def mutation_token(self) -> Optional[MutationToken]:
        """Get the operation's mutation token, if it exists.

        Returns:
            Optional[:class:`.MutationToken`]: The operation's mutation token.
        """
        if self._raw_mutation_token is not None and self._mutation_token is None:
            self._mutation_token = MutationToken(self._raw_mutation_token.get())
        return self._mutation_token

    def __repr__(self):
        return 'MutationResult:{}'.format(self._orig)

# Sub-document Operations

class ContentSubdocProxy:
    """
    Used to provide access to LookUpResult content via Result.content_as[type](index)
    """

    def __init__(self, content, key):
        self._content = content
        self._key = key

    def _parse_content_at_index(self, index, type_):
        if index > len(self._content) - 1 or index < 0:
            raise InvalidIndexException(
                f"Provided index ({index}) is invalid.")

        item = self._content[index].get("value", None)
        if item is None:
            # TODO:  implement exc_from_rc()??
            status = self._content[index].get("status", None)
            if not status:
                raise DocumentNotFoundException(
                    f"Could not find document for key: {self._key}")
            if status == SubDocStatus.PathNotFound:
                path = self._content[index].get("path", None)
                raise PathNotFoundException(
                    f"Path ({path}) could not be found for key: {self._key}")
            if status == SubDocStatus.PathMismatch:
                path = self._content[index].get("path", None)
                raise PathMismatchException(
                    f"Path ({path}) mismatch for key: {self._key}")
            if status == SubDocStatus.ValueCannotInsert:
                path = self._content[index].get("path", None)
                raise SubdocCantInsertValueException(
                    f"Cannot insert value at path ({path}) for key: {self._key}")
            if status == SubDocStatus.PathExists:
                path = self._content[index].get("path", None)
                raise PathExistsException(
                    f"Path ({path}) already exists for key: {self._key}")

        return type_(item)

    def __getitem__(self,
                    type_       # type: Any
                    ) -> Any:
        """

        :param type_: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return lambda index: self._parse_content_at_index(index, type_)

class LookupInResult(LookupInResultInterface):
    def __init__(self, orig # type: result
        ):
        self._orig = orig

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get('cas', None)

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a dict::

                res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
                value = res.content_as[dict](0)
        """
        return ContentSubdocProxy(self.value, self.key)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get('key', None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._orig.raw_result.get('value', None)

    def exists(self,
               index  # type: int
               ) -> bool:
        """Check if the subdocument path exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the provided index is out of range.

        Returns:
            bool: True if the path exists.  False if the path does not exist.
        """

        if index > len(self.value) - 1 or index < 0:
            raise InvalidIndexException(
                f"Provided index ({index}) is invalid.")

        exists = self.value[index].get("exists", None)
        return exists is not None and exists is True

    def __repr__(self):
        return "LookupInResult:{}".format(self._orig)


class MutateInResult(MutateInResultInterface):
    def __init__(self, 
                orig # type: result
                ):
        self._orig = orig
        self._raw_mutation_token = self._orig.raw_result.get('mutation_token', None)
        self._mutation_token = None

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get('cas', None)

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a str::

                res = collection.mutate_in(key, (SD.upsert("city", "New City"),
                                                SD.replace("faa", "CTY")))
                value = res.content_as[str](0)
        """
        return ContentSubdocProxy(self.value, self.key)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get('key', None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    def mutation_token(self) -> Optional[MutationToken]:
        """Get the operation's mutation token, if it exists.

        Returns:
            Optional[:class:`.MutationToken`]: The operation's mutation token.
        """
        if self._raw_mutation_token is not None and self._mutation_token is None:
            self._mutation_token = MutationToken(self._raw_mutation_token.get())
        return self._mutation_token

    def __repr__(self):
        return "MutateInResult:{}".format(self._orig)


"""

Python SDK Streaming Results

"""

class QueryResult(QueryResultInterface):
    def __init__(self,
                 n1ql_request # type: N1QLRequest
    ):
        self._request = n1ql_request

    def rows(self):
        """The rows which have been returned by the query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        return self.__iter__()

    def execute(self):
        """Convenience method to execute the query.

        Returns:
            List[Any]:  A list of query results.

        Example:
            q_rows = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;').execute()

        """
        return self._request.execute()

    def metadata(self):
        """The meta-data which has been returned by the query.

        Returns:
            :class:`~couchbase.n1ql.QueryMetaData`: An instance of :class:`~couchbase.n1ql.QueryMetaData`.
        """
        return self._request.metadata()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        raise NotImplementedError('Not an async QueryResult.')

    def __repr__(self):
        return "QueryResult:{}".format(self._request)