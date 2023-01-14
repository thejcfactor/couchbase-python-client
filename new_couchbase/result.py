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

from new_couchbase.api.result import DiagnosticsResultInterface, GetResultInterface, MutationResultInterface, PingResultInterface
from new_couchbase.common.diagnostics import ClusterState, EndpointDiagnosticsReport, EndpointPingReport, EndpointState, ServiceType
from new_couchbase.common.mutation_state import MutationToken

"""

Python SDK Diagnostic Operation Results

"""

class ClusterInfoResult:
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
            tokens = self._server_version_raw.split("-")
            if len(tokens) == 3:
                self._is_community = tokens[2].upper() == "COMMUNITY"

        return self._is_community

    @property
    def is_enterprise(self) -> Optional[bool]:
        """
            bool: True if connected Couchbase Server is Enterprise edition, false otherwise.
        """
        if not self._is_enterprise:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split("-")
            if len(tokens) == 3:
                self._is_enterprise = tokens[2].upper() == "ENTERPRISE"

        return self._is_enterprise

    @property
    def nodes(self):
        return self._raw_result.get("nodes", None)

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
            tokens = self._server_version_raw.split("-")
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
            v = n["version"]
            if version is None:
                version = v
            elif v != version:
                # mixed versions -- not supported
                version = None
                break

        self._server_version_raw = version

    def __repr__(self):
        return "ClusterInfoResult:{}".format(self._raw_result)

class DiagnosticsResult(DiagnosticsResultInterface):
    def __init__(self, result # type: Dict[str, Any]
        ):
        self._raw_result = result
        svc_endpoints = self._raw_result.get("endpoints", None)
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
        return self._raw_result.get("id", None)

    @property
    def sdk(self) -> str:
        """
            str: The name of the SDK which generated this report.
        """
        return self._raw_result.get("sdk", None)

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
        return self._raw_result.get("version", None)


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
        return "DiagnosticsResult:{}".format(self._raw_result)


class PingResult(PingResultInterface):
    def __init__(self, result # type: Dict[str, Any]
        ):
        self._raw_result = result
        svc_endpoints = self._raw_result.get("endpoints", None)
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
        return self._raw_result.get("id", None)

    @property
    def sdk(self) -> str:
        """
            str: The name of the SDK which generated this report.
        """
        return self._raw_result.get("sdk", None)

    @property
    def success(self) -> bool:
        return self._raw_result is not None and len(self._raw_result) > 0

    @property
    def version(self) -> int:
        """
            int: The version number of this report.
        """
        return self._raw_result.get("version", None)

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
        return "PingResult:{}".format(self._raw_result)


"""

Python SDK Key-Value Operation Results

"""


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