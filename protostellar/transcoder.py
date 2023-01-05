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

from abc import ABC, abstractmethod
from typing import (TYPE_CHECKING,
                    Any,
                    Tuple)

from couchbase.exceptions import ValueFormatException
from couchbase.serializer import DefaultJsonSerializer

from protostellar.proto.couchbase.kv import v1_pb2

if TYPE_CHECKING:
    from couchbase.serializer import Serializer


class Transcoder(ABC):
    """Interface a Custom Transcoder must implement
    """

    @abstractmethod
    def encode_value(self,
                     value  # type: Any
                     ) -> Tuple[bytes, int]:
        raise NotImplementedError()

    @abstractmethod
    def decode_value(self,
                     value,  # type: bytes
                     content_type  # type: int
                     ) -> Any:
        raise NotImplementedError()

class JSONTranscoder(Transcoder):

    def __init__(self, serializer=None  # type: Serializer
                 ):

        if not serializer:
            self._serializer = DefaultJsonSerializer()
        else:
            self._serializer = serializer

    def encode_value(self,
                     value,  # type: Any
                     ) -> Tuple[bytes, int]:

        if isinstance(value, str):
            format = v1_pb2.JSON
        elif isinstance(value, (bytes, bytearray)):
            raise ValueFormatException(
                "The JSONTranscoder (default transcoder) does not support binary data.")
        elif isinstance(value, (list, tuple, dict, bool, int, float)) or value is None:
            format = v1_pb2.JSON
        else:
            raise ValueFormatException(
                "Unrecognized value type {}".format(type(value)))

        if format != v1_pb2.JSON:
            raise ValueFormatException("Unrecognized format {}".format(format))

        return self._serializer.serialize(value), v1_pb2.JSON

    def decode_value(self,
                     value,  # type: bytes
                     content_type  # type: int
                     ) -> Any:

        if content_type == v1_pb2.JSON:
            return self._serializer.deserialize(value)
        elif content_type == v1_pb2.BINARY:
            raise ValueFormatException(
                "The JSONTranscoder (default transcoder) does not support binary format")
        else:
            raise ValueFormatException(
                "Unrecognized format provided: {}".format(format))