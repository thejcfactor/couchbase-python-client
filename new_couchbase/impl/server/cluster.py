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

from new_couchbase.api.cluster import ClusterInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.transcoder import TranscoderInterface

from new_couchbase.api import ApiImplementation

from new_couchbase.bucket import Bucket
from new_couchbase.impl.server.core import BlockingWrapper

from new_couchbase.impl.server.exceptions import exception as BaseCouchbaseException
from new_couchbase.impl.server.exceptions import ErrorMapper

from new_couchbase.result import ClusterInfoResult, DiagnosticsResult, PingResult

if TYPE_CHECKING:
    from new_couchbase.options import DiagnosticsOptions, PingOptions
    from new_couchbase.api.cluster import ClusterCoreInterface

class Cluster(ClusterInterface):

    def __init__(self,
                 core: ClusterCoreInterface
                 ) -> Cluster:
        self._core = core
        self._connect()

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.SERVER

    @property
    def connected(self) -> bool:
        """
            bool: Indicator on if the cluster has been connected or not.
        """
        return self._core.connected

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._core.connection

    @property
    def default_transcoder(self) -> TranscoderInterface:
        """
        **INTERNAL**
        """
        return self._core._default_transcoder

    @property
    def default_serializer(self) -> SerializerInterface:
        return self._core._default_serializer

    @property
    def server_version(self) -> Optional[str]:
        return self._core.server_version

    @property
    def server_version_short(self) -> Optional[float]:
        return self._core.server_version_short

    @property
    def server_version_full(self) -> Optional[str]:
        return self._core.server_version_full

    @property
    def is_developer_preview(self) -> Optional[bool]:
        return self._core.is_developer_preview

    @BlockingWrapper.block(True)
    def _close_cluster(self):
        self._core.close_cluster()
        self._core.destroy_connection()

    @BlockingWrapper.block(True)
    def _connect(self, **kwargs):
        ret = self._core.connect_cluster(**kwargs)
        if isinstance(ret, BaseCouchbaseException):
            raise ErrorMapper.build_exception(ret)
        self._core.set_connection(ret)

    @BlockingWrapper.block(ClusterInfoResult)
    def _get_cluster_info(self):
        cluster_info = self._core.get_cluster_info()
        self._core.cluster_info = cluster_info

    def bucket(self,
               bucket_name # type: str
               ) -> Bucket:
        """Creates a Bucket instance to a specific bucket.

        .. seealso::
            :class:`.bucket.Bucket`

        Args:
            bucket_name (str): Name of the bucket to reference

        Returns:
            :class:`~couchbase.bucket.Bucket`: A bucket instance

        Raises:
            RuntimeError:  If called prior to the cluster being connected.
            :class:`~couchbase.exceptions.BucketNotFoundException`: If provided `bucket_name` cannot
                be found.

        """
        if not self.connected:
            raise RuntimeError("Cluster not yet connected.")

        return Bucket(self, bucket_name)

    @BlockingWrapper.block(DiagnosticsResult)
    def diagnostics(self,
                    *opts,  # type: DiagnosticsOptions
                    **kwargs  # type: Dict[str, Any]
                    ) -> DiagnosticsResult:
        """Performs a diagnostic operation against the cluster.

        The diagnostic operations returns a report about the current active connections with the cluster.
        Includes information about remote and local addresses, last activity, and other diagnostics information.

        Args:
            opts (:class:`~couchbase.options.DiagnosticsOptions`): Optional parameters for this operation.

        Returns:
            :class:`~couchbase.result.DiagnosticsResult`: A report which describes current active connections
            with the cluster.

        """

        return self._core.diagnostics(*opts, **kwargs)

    def get_cluster_info(self, refresh=False # type: Optional[bool]
        ) -> ClusterInfoResult:
        """Retrieve the Couchbase cluster information

        .. note::
            If using Couchbase Server version < 6.6, a bucket *must* be opened prior to calling
            `cluster.cluster_info()`.  If a bucket is not opened a
            :class:`~couchbase.exceptions.ServiceUnavailableException` will be raised.


        Returns:
            :class:`~couchbase.result.ClusterInfoResult`: Information about the connected cluster.

        Raises:
            RuntimeError:  If called prior to the cluster being connected.
            :class:`~couchbase.exceptions.ServiceUnavailableException`: If called prior to connecting
                to a bucket if using server version < 6.6.

        """
        if not self.connected:
            raise RuntimeError(
                "Cluster is not connected, cannot get cluster info.")

        if self._core.cluster_info and not refresh:
            return self._core.cluster_info

        self._get_cluster_info()
        return self._core.cluster_info

    @BlockingWrapper.block(PingResult)
    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Any
             ) -> PingResult:
        """Performs a ping operation against the cluster.

        The ping operation pings the services which are specified
        (or all services if none are specified). Returns a report which describes the outcome of
        the ping operations which were performed.

        Args:
            opts (:class:`~couchbase.options.PingOptions`): Optional parameters for this operation.

        Returns:
            :class:`~couchbase.result.PingResult`: A report which describes the outcome of the ping operations
            which were performed.

        """
        return self._core.ping(*opts, **kwargs)