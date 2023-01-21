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

from new_couchbase.api import ApiImplementation

from new_couchbase.bucket import Bucket
from new_couchbase.classic.core import BlockingWrapper
from new_couchbase.classic.core.cluster import ClusterCore

from new_couchbase.classic.exceptions import exception as BaseCouchbaseException
from new_couchbase.classic.exceptions import ErrorMapper
from new_couchbase.classic.core.n1ql import N1QLQuery
from new_couchbase.classic.n1ql import N1QLRequest

from new_couchbase.classic.result import (ClusterInfoResult,
                                   DiagnosticsResult,
                                   PingResult,
                                   QueryResult)

if TYPE_CHECKING:
    from new_couchbase.options import (ClusterOptions,
                                        DiagnosticsOptions,
                                        PingOptions,
                                        QueryOptions)

class Cluster(ClusterCore):

    def __init__(self,
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> None:
        super().__init__(connstr, *options, **kwargs)
        self._connect()

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.CLASSIC




    @BlockingWrapper.block(True)
    def _close_cluster(self):
        self.close_cluster()
        self.destroy_connection()

    @BlockingWrapper.block(True)
    def _connect(self, **kwargs):
        ret = self.connect_cluster(**kwargs)
        if isinstance(ret, BaseCouchbaseException):
            raise ErrorMapper.build_exception(ret)
        self.set_connection(ret)

    @BlockingWrapper.block(ClusterInfoResult)
    def _get_cluster_info(self):
        return super().get_cluster_info()

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

    def close(self):
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        if self.connected:
            self._close_cluster()

    def cluster_info(self) -> ClusterInfoResult:
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
        cluster_info = None
        cluster_info = self._get_cluster_info()
        self._cluster_info = cluster_info
        return cluster_info

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

        return self.diagnostics(*opts, **kwargs)

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

        if super().cluster_info and not refresh:
            return super().cluster_info

        super().cluster_info = self._get_cluster_info()

    @BlockingWrapper.block(PingResult)
    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str, Any]
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
        return self.ping(*opts, **kwargs)

    def query(
        self,
        statement,  # type: str
        *opts,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> QueryResult:
        query = N1QLQuery.create_query_object(statement,
                                              *opts,
                                              **kwargs)
        return QueryResult(N1QLRequest.generate_n1ql_request(self.connection,
                                                query.params,
                                                default_serializer=self.default_serializer))

    @staticmethod
    def connect(connstr,  # type: str
                options,  # type: ClusterOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> Cluster:

        cluster = Cluster(connstr, options, **kwargs)
        return cluster