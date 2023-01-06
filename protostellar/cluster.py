#  Copyright 2016-2022. Couchbase, Inc.
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
                    List,
                    Optional,
                    Tuple)

import grpc

from couchbase.serializer import DefaultJsonSerializer, Serializer
from protostellar.bucket import Bucket
from protostellar.management.buckets import BucketManager
from protostellar.options import ConnectOptions
from protostellar.transcoder import JSONTranscoder

if TYPE_CHECKING:
    from couchbase.transcoder import Transcoder


class Cluster:
    def __init__(self,
                 connstr,  # type: str
                 options,  # type: ConnectOptions
                 **kwargs,  # type: Dict[str, Any]
                 ):
        # with open('/Users/jaredcasey/Desktop/server_cert.pem', 'rb') as f:
        #     self._creds = grpc.ssl_channel_credentials(f.read())
        # self._creds = grpc.ssl_channel_credentials(creds)
        # self._channel = grpc.secure_channel(host, self._creds)
        # self._creds = grpc.composite_channel_credentials(grpc.ssl_channel_credentials(), options.grpc_call_credentials())        
        # self._channel = grpc.secure_channel(connstr, self._creds)

        self._metadata = []
        self._metadata.append(options.auth_metadata())
        self._channel = grpc.insecure_channel(connstr)
        self._default_serializer = DefaultJsonSerializer()
        self._default_transcoder = JSONTranscoder()

    @property
    def channel(self):
        """
        **INTERNAL**
        """
        return self._channel

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        """
        **INTERNAL**
        """
        return self._default_transcoder

    @property
    def default_serializer(self) -> Optional[Serializer]:
        return self._default_serializer

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    def bucket(self, name,  # type: str
               ) -> Bucket:
        return Bucket(self, name, self._metadata)

    def buckets(self) -> BucketManager:
        """
        Get a :class:`~couchbase.management.buckets.BucketManager` which can be used to manage the buckets
        of this cluster.

        Returns:
            :class:`~couchbase.management.buckets.BucketManager`: A :class:`~couchbase.management.buckets.BucketManager` instance.
        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return BucketManager(self)

    def buckets(self) -> BucketManager:
        """
        Get a :class:`~couchbase.management.buckets.BucketManager` which can be used to manage the buckets
        of this cluster.

        Returns:
            :class:`~couchbase.management.buckets.BucketManager`: A :class:`~couchbase.management.buckets.BucketManager` instance.
        """  # noqa: E501
        # TODO:  AlreadyShutdownException?
        return BucketManager(self)

    @staticmethod
    def connect(connstr,  # type: str
                options,  # type: ConnectOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> Cluster:

        cluster = Cluster(connstr, options, **kwargs)
        return cluster
