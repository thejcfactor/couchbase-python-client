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

import base64
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Tuple)

import grpc

from new_couchbase.bucket import Bucket
from new_couchbase.api import ApiImplementation
from new_couchbase.serializer import DefaultJsonSerializer, Serializer
from new_couchbase.transcoder import JSONTranscoder, Transcoder
from new_couchbase.common.connection_string import parse_connection_string, parse_legacy_query_options
from new_couchbase.common.options import OptionTypes, parse_options

from new_couchbase.result import (DiagnosticsResult,
                              PingResult,
                              QueryResult)

from new_couchbase.exceptions import InvalidArgumentException, FeatureUnavailableException
from new_couchbase.protostellar.n1ql import N1QLQuery, N1QLRequest
from new_couchbase.protostellar.options import ValidClusterOptions
from protostellar import query_grpc_module as query



if TYPE_CHECKING:
    from new_couchbase.options import (ClusterOptions,
                                       DiagnosticsOptions,
                                       PingOptions,
                                       QueryOptions)

class Cluster:

    def __init__(self,  # noqa: C901
                 connstr,  # type: str
                 *options,  # type: ClusterOptions
                 **kwargs  # type: Dict[str, Any]
                 ):

        # parse query string prior to parsing ClusterOptions
        connection_str, query_opts, legacy_opts = parse_connection_string(connstr)
        self._connstr = connection_str.replace('protostellar://', '')

        kwargs.update(query_opts)
        cluster_opts = parse_options(ValidClusterOptions.get_valid_options(OptionTypes.Cluster), kwargs, *options)
        # add legacy options after parsing ClusterOptions to keep logic separate
        cluster_opts.update(parse_legacy_query_options(**legacy_opts))

        authenticator = cluster_opts.pop("authenticator", None)
        if not authenticator:
            raise InvalidArgumentException(message="Authenticator is mandatory.")

        self._metadata = []
        self._auth = authenticator.as_dict()
        self._set_auth_metadata()
        self._channel = grpc.insecure_channel(self._connstr)
        self._default_serializer = DefaultJsonSerializer()
        self._default_transcoder = JSONTranscoder()
        self._query_service = query.QueryStub(self.connection)

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.PROTOSTELLAR

    @property
    def connected(self) -> bool:
        """
            bool: Indicator on if the cluster has been connected or not.
        """
        return hasattr(self, "_channel") and self._channel is not None

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        if not hasattr(self, "_channel"):
            self._channel = None
        return self._channel

    @property
    def default_transcoder(self) -> Transcoder:
        return self._default_transcoder

    @property
    def default_serializer(self) -> Serializer:
        return self._default_serializer

    @property
    def metadata(self) -> List[Tuple[str]]:
        """
        **INTERNAL**
        """
        return self._metadata
    
    @property
    def query_service(self):
        """
        **INTERNAL**
        """
        return self._query_service

    @property
    def server_version(self) -> Optional[str]:
        return '7.5.0-1234'

    @property
    def server_version_short(self) -> Optional[float]:
        return 7.5

    @property
    def server_version_full(self) -> Optional[str]:
        return '7.5.0'

    @property
    def is_developer_preview(self) -> Optional[bool]:
        return False

    def _set_auth_metadata(self) -> None:
        username = self._auth.get('username', None)
        pw = self._auth.get('password', None)
        auth = f'{username}:{pw}'.encode(encoding='utf-8')
        token = base64.b64encode(auth)
        auth_str = f'Basic {token.decode(encoding="utf-8")}'
        # all metadata keys must be lowercase: https://github.com/grpc/grpc/issues/9863
        self._metadata.append(('authorization', auth_str))

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

        """
        if self.connected:
            self._channel.close()

    def cluster_info(self):
        raise FeatureUnavailableException

    def diagnostics(self,
                *opts,  # type: Optional[DiagnosticsOptions]
                **kwargs  # type: Dict[str, Any]
                ) -> DiagnosticsResult:
        raise NotImplementedError

    def ping(self,
             *opts,  # type: Optional[PingOptions]
             **kwargs  # type: Dict[str, Any]
             ) -> PingResult:
        raise NotImplementedError

    def query(
        self,
        statement,  # type: str
        *opts,  # type: QueryOptions
        **kwargs  # type: Dict[str, Any]
    ) -> QueryResult:
        # raise FeatureUnavailableException(message=("Protostellar does not support query operations from a cluster object."
        #                                            " Use scope.query()."))
        query = N1QLQuery.create_query_object(statement,
                                              *opts,
                                              **kwargs)
        return QueryResult(N1QLRequest.generate_n1ql_request(self.query_service, query.params))