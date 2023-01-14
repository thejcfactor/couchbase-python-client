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

from new_couchbase.impl import ClusterFactory
from new_couchbase.api.cluster import ClusterInterface
from new_couchbase.common.supportability import Supportability
from new_couchbase.api import ApiImplementation

from new_couchbase.result import DiagnosticsResult, PingResult
from new_couchbase.api.bucket import BucketInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.transcoder import TranscoderInterface

if TYPE_CHECKING:
    from new_couchbase.options import ClusterOptions, DiagnosticsOptions, PingOptions

class Cluster(ClusterInterface):

    def __init__(self,
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> Cluster:
        self._impl = ClusterFactory.create_cluster(connstr, *options, **kwargs)

    @property
    def api_implementation(self) -> ApiImplementation:
        return self._impl.api_implementation

    @property
    def connected(self) -> bool:
        return self._impl.connected

    @property
    def default_transcoder(self) -> TranscoderInterface:
        return self._impl.default_transcoder

    @property
    def default_serializer(self) -> SerializerInterface:
        return self._impl.default_serializer

    @property
    def server_version(self) -> Optional[str]:
        return self._impl.server_version

    @property
    def server_version_short(self) -> Optional[float]:
        return self._impl.server_version_short

    @property
    def server_version_full(self) -> Optional[str]:
        return self._impl.server_version_full

    @property
    def is_developer_preview(self) -> Optional[bool]:
        return self._impl.is_developer_preview

    def bucket(self,
               bucket_name # type: str
               ) -> BucketInterface:
        return self._impl.bucket(bucket_name)

    def diagnostics(self,
                    *opts,  # type: Optional[DiagnosticsOptions]
                    **kwargs  # type: Dict[str, Any]
                    ) -> DiagnosticsResult:
        return self._impl.diagnostics(*opts, **kwargs)

    def ping(self,
             *opts,  # type: Optional[PingOptions]
             **kwargs  # type: Dict[str, Any]
             ) -> PingResult:
        return self._impl.ping(*opts, **kwargs)

    @staticmethod
    def connect(connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> Cluster:
        """Create a Couchbase Cluster and connect

        Args:
            connstr (str):
                The connection string to use for connecting to the cluster.
                This is a URI-like string allowing specifying multiple hosts.

                The format of the connection string is the *scheme*
                (``couchbase`` for normal connections, ``couchbases`` for
                SSL enabled connections); a list of one or more *hostnames*
                delimited by commas
            options (:class:`~couchbase.options.ClusterOptions`): Global options to set for the cluster.
                Some operations allow the global options to be overriden by passing in options to the
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                overrride provided :class:`~couchbase.options.ClusterOptions`

        Returns:
            :class:`.Cluster`: If successful, a connect Couchbase Cluster instance.
        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If no :class:`~couchbase.auth.Authenticator`
                is provided.  Also raised if an invalid `ClusterOption` is provided.
            :class:`~couchbase.exceptions.AuthenticationException`: If provided :class:`~couchbase.auth.Authenticator`
                has incorrect credentials.


        Examples:
            Initialize cluster using default options::

                from couchbase.auth import PasswordAuthenticator
                from couchbase.cluster import Cluster
                from couchbase.options import ClusterOptions

                auth = PasswordAuthenticator('username', 'password')
                cluster = Cluster.connect('couchbase://localhost', ClusterOptions(auth))

            Connect using SSL::

                from couchbase.auth import PasswordAuthenticator
                from couchbase.cluster import Cluster
                from couchbase.options import ClusterOptions

                auth = PasswordAuthenticator('username', 'password', cert_path='/path/to/cert')
                cluster = Cluster.connect('couchbases://localhost', ClusterOptions(auth))

            Initialize cluster using with global timeout options::

                from datetime import timedelta

                from couchbase.auth import PasswordAuthenticator
                from couchbase.cluster import Cluster
                from couchbase.options import ClusterOptions, ClusterTimeoutOptions

                auth = PasswordAuthenticator('username', 'password')
                timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10),
                                                    query_timeout=timedelta(seconds=120))
                cluster = Cluster.connect('couchbase://localhost', ClusterOptions(auth, timeout_options=timeout_opts))

        """
        cluster = Cluster(connstr, *options, **kwargs)
        return cluster
        

"""
** DEPRECATION NOTICE **

The classes below are deprecated for 3.x compatibility.  They should not be used.
Instead use:
    * All options should be imported from `couchbase.options`.
    * All options Enums should be imported from `couchbase.options`.
    * All N1QL Enums should be imported from `couchbase.n1ql`.

"""

from new_couchbase.common.options import ClusterOptionsBase  # nopep8 # isort:skip # noqa: E402
from new_couchbase.common.options import ClusterTimeoutOptionsBase  # nopep8 # isort:skip # noqa: E402
from new_couchbase.common.options import ClusterTracingOptionsBase  # nopep8 # isort:skip # noqa: E402
# from couchbase.logic.options import DiagnosticsOptionsBase  # nopep8 # isort:skip # noqa: E402
# from couchbase.logic.options import QueryOptionsBase  # nopep8 # isort:skip # noqa: E402


@Supportability.import_deprecated('couchbase.cluster', 'couchbase.options')  # noqa: F811
class ClusterOptions(ClusterOptionsBase):  # noqa: F811
    pass


@Supportability.import_deprecated('couchbase.cluster', 'couchbase.options')  # noqa: F811
class ClusterTimeoutOptions(ClusterTimeoutOptionsBase):
    pass


@Supportability.import_deprecated('couchbase.cluster', 'couchbase.options')  # noqa: F811
class ClusterTracingOptions(ClusterTracingOptionsBase):
    pass