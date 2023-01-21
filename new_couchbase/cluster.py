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

from new_couchbase.bucket import Bucket
from new_couchbase.common.supportability import Supportability
from new_couchbase.api import ApiImplementation
from new_couchbase.n1ql import N1QLRequest

from new_couchbase.result import (DiagnosticsResult,
                                  PingResult,
                                  QueryResult)


if TYPE_CHECKING:
    from new_couchbase.options import (ClusterOptions,
                                        DiagnosticsOptions,
                                        PingOptions,
                                        QueryOptions)

class Cluster:

    def __init__(self,
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs,  # type: Dict[str, Any]
                 ) -> Cluster:

        if connstr.startswith('protostellar://'):
            from new_couchbase.protostellar.cluster import Cluster as ProtostellarCluster
            self._impl = ProtostellarCluster(connstr, *options, **kwargs)
        else:
            from new_couchbase.classic.cluster import Cluster as ClassicCluster
            self._impl = ClassicCluster(connstr, *options, **kwargs)

    @property
    def api_implementation(self) -> ApiImplementation:
        return self._impl.api_implementation

    @property
    def connected(self) -> bool:
        return self._impl.connected

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
               ) -> Bucket:
        return self._impl.bucket(bucket_name)

    def close(self):
        """Shuts down this cluster instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Cluster resources should be cleaned
            up once the cluster instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        self._impl.close()

    def cluster_info(self):
        return self._impl.cluster_info()

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

    def query(
        self,
        statement,  # type: str
        *opts,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> QueryResult:
        """Executes a N1QL query against the cluster.

        .. note::
            The query is executed lazily in that it is executed once iteration over the
            :class:`~couchbase.result.QueryResult` begins.

        .. seealso::
            * :class:`~couchbase.management.queries.QueryIndexManager`: for how to manage query indexes
            * :meth:`couchbase.Scope.query`: For how to execute scope-level queries.

        Args:
            statement (str): The N1QL statement to execute.
            options (:class:`~couchbase.options.QueryOptions`): Optional parameters for the query operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.QueryOptions`

        Returns:
            :class:`~couchbase.result.QueryResult`: An instance of a :class:`~couchbase.result.QueryResult` which
            provides access to iterate over the query results and access metadata and metrics about the query.

        Examples:
            Simple query::

                q_res = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;')
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with positional parameters::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $1 LIMIT $2;'
                q_res = cluster.query(q_str, QueryOptions(positional_parameters=['United%', 5]))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Simple query with named parameters::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = cluster.query(q_str, QueryOptions(named_parameters={'country': 'United%', 'lim':2}))
                for row in q_res.rows():
                    print(f'Found row: {row}')

            Retrieve metadata and/or metrics from query::

                from couchbase.options import QueryOptions

                # ... other code ...

                q_str = 'SELECT * FROM `travel-sample` WHERE country LIKE $country LIMIT $lim;'
                q_res = cluster.query(q_str, QueryOptions(metrics=True))
                for row in q_res.rows():
                    print(f'Found row: {row}')

                print(f'Query metadata: {q_res.metadata()}')
                print(f'Query metrics: {q_res.metadata().metrics()}')

        """
        return QueryResult(self._impl.query(statement, *opts, **kwargs))

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