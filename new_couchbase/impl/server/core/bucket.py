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
                    Dict)

from new_couchbase.common.options import parse_options, OptionTypes
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.result import PingResult
from new_couchbase.impl.server.options import ValidDiagnosticOptions

from couchbase.pycbc_core import (diagnostics_operation,
                                  open_or_close_bucket,
                                  operations)

if TYPE_CHECKING:
    from new_couchbase.options import PingOptions
    from new_couchbase.api.cluster import ClusterInterface

class BucketCore:

    def __init__(self, 
                cluster, # type: ClusterInterface
                bucket_name, # type: str
                ):
        self._cluster = cluster
        self._connection = cluster.connection
        self._bucket_name = bucket_name
        self._connected = False

    @property
    def connected(self) -> bool:
        """
            bool: Indicator on if the cluster has been connected or not.
        """
        return self._connected

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._connection

    @property
    def default_serializer(self) -> SerializerInterface:
        return self._cluster.default_serializer

    @property
    def default_transcoder(self) -> TranscoderInterface:
        return self._cluster.default_transcoder

    @property
    def name(self) -> str:
        """
            str: The name of this :class:`~.Bucket` instance.
        """
        return self._bucket_name

    def destroy_connection(self):
        del self._cluster
        del self._connection

    def open_or_close_bucket(self, open_bucket=True, **kwargs):
        if not self._connection:
            raise RuntimeError("No cluster connection")

        bucket_kwargs = {
            "open_bucket": 1 if open_bucket is True else 0
        }

        callback = kwargs.pop('callback', None)
        if callback:
            bucket_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            bucket_kwargs['errback'] = errback

        return open_or_close_bucket(self._connection, self._bucket_name, **bucket_kwargs)

    def set_connected(self, value):
        self._connected = value

    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str,Any]
             ) -> PingResult:

        ping_kwargs = {
            'conn': self._connection,
            'op_type': operations.PING.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            ping_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            ping_kwargs['errback'] = errback

        final_args = parse_options(ValidDiagnosticOptions.get_valid_options(OptionTypes.Ping), kwargs, *opts)
        # TODO: tracing
        # final_args.pop("span", None)

        ping_kwargs.update(final_args)
        return diagnostics_operation(**ping_kwargs)