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

import json
import pickle  # nosec
from abc import ABC, abstractmethod
from enum import IntEnum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional,
                    Tuple,
                    Union)

from new_couchbase.serializer import DefaultJsonSerializer
from new_couchbase.api import ApiImplementation

from new_couchbase.exceptions import (CouchbaseException,
                                      InvalidArgumentException,
                                      ValueFormatException)

if TYPE_CHECKING:
    from new_couchbase.serializer import Serializer


class TranscoderFormat(IntEnum):
    FMT_JSON = 0
    FMT_BYTES = 1
    FMT_STRING = 2


def get_format_flags(format, # type: TranscoderFormat
                    implementation=None, # type: Optional[Union[ApiImplementation, int]]
    ) -> int:

    if not implementation:
        implementation = ApiImplementation.CLASSIC

    if implementation == ApiImplementation.PROTOSTELLAR:
        try:
            from new_couchbase.protostellar.proto.couchbase.kv import v1_pb2
            if format == TranscoderFormat.FMT_JSON:
                return v1_pb2.JSON
            elif format == TranscoderFormat.FMT_BYTES:
                return v1_pb2.BINARY
            else:
                return v1_pb2.UNKNOWN
        except ImportError as ex:
            raise CouchbaseException(message="Unable to import couchbase protostellar package.", 
                                    exc_info={'inner_cause': ex})
    else:
        try:
            import new_couchbase.classic.core.transcoder as classic_transcoder

            if format == TranscoderFormat.FMT_JSON:
                return classic_transcoder.FMT_JSON
            elif format == TranscoderFormat.FMT_BYTES:
                return classic_transcoder.FMT_BYTES
            elif format == TranscoderFormat.FMT_STRING:
                return classic_transcoder.FMT_UTF8
        except ImportError as ex:
            raise CouchbaseException(message="Unable to import couchbase classic package.", 
                                    exc_info={'inner_cause': ex})

def get_decode_format(flags, # type: int
                    implementation=None # type: Optional[Union[ApiImplementation, int]]
    ) -> int:

    if not implementation:
        implementation = ApiImplementation.CLASSIC

    if implementation == ApiImplementation.PROTOSTELLAR:
        try:
            from new_couchbase.protostellar.proto.couchbase.kv import v1_pb2
            if flags == v1_pb2.JSON:
                return v1_pb2.JSON
            elif flags == v1_pb2.BINARY:
                return v1_pb2.BINARY
            else:
                return v1_pb2.UNKNOWN
        except ImportError as ex:
            raise CouchbaseException(message="Unable to import couchbase protostellar package.", 
                                    exc_info={'inner_cause': ex})
    else:
        try:
            from new_couchbase.classic.core.transcoder import get_decode_format
            return get_decode_format(flags)
        except ImportError as ex:
            raise CouchbaseException(message="Unable to import couchbase classic package.", 
                                    exc_info={'inner_cause': ex})

class Transcoder(ABC):
    """Interface a Custom Transcoder must implement
    """

    @abstractmethod
    def encode_value(self,
                     value,   # type: Any
                     **kwargs, # type: Dict[str, Any]
                     ) -> Tuple[bytes, int]:
        raise NotImplementedError

    @abstractmethod
    def decode_value(self,
                     value,  # type: bytes
                     content_type,  # type: int
                     **kwargs, # type: Dict[str, Any]
                     ) -> Any:
        raise NotImplementedError

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'encode_value') and
                callable(subclass.encode_value) and
                hasattr(subclass, 'decode_value') and
                callable(subclass.decode_value))

class JSONTranscoder(Transcoder):

    def __init__(self, serializer=None  # type: Serializer
                 ):

        if not serializer:
            self._serializer = DefaultJsonSerializer()
        else:
            self._serializer = serializer

    def encode_value(self,
                     value,  # type: Any
                     **kwargs, # type: Dict[str, Any]
                     ) -> Tuple[bytes, int]:

        fmt_json = get_format_flags(TranscoderFormat.FMT_JSON, kwargs.get('implementation', None))
        if isinstance(value, str):
            format = fmt_json
        elif isinstance(value, (bytes, bytearray)):
            raise ValueFormatException(
                "The JSONTranscoder (default transcoder) does not support binary data.")
        elif isinstance(value, (list, tuple, dict, bool, int, float)) or value is None:
            format = fmt_json
        else:
            raise ValueFormatException(
                "Unrecognized value type {}".format(type(value)))

        if format != fmt_json:
            raise ValueFormatException("Unrecognized format {}".format(format))

        return self._serializer.serialize(value), fmt_json

    def decode_value(self,
                     value,  # type: bytes
                     flags,  # type: int
                     **kwargs, # type: Dict[str, Any]
                     ) -> Any:

        impl = kwargs.get('implementation', None)
        format = get_decode_format(flags, impl)

        if format == get_format_flags(TranscoderFormat.FMT_BYTES, impl):
            raise ValueFormatException(
                "The JSONTranscoder (default transcoder) does not support binary format")
        elif format == get_format_flags(TranscoderFormat.FMT_STRING, impl):
            raise ValueFormatException(
                "The JSONTranscoder (default transcoder) does not support string format")
        elif format == get_format_flags(TranscoderFormat.FMT_JSON, impl):
            return self._serializer.deserialize(value)
        else:
            raise ValueFormatException(
                "Unrecognized format provided: {}".format(format))


class RawJSONTranscoder(Transcoder):

    def encode_value(self,
                     value,  # type: Union[str,bytes,bytearray]
                     **kwargs, # type: Dict[str, Any]
                     ) -> Tuple[bytes, int]:

        fmt_json = get_format_flags(TranscoderFormat.FMT_JSON, kwargs.get('implementation', None))

        if isinstance(value, str):
            return value.encode('utf-8'), fmt_json
        elif isinstance(value, (bytes, bytearray)):
            if isinstance(value, bytearray):
                value = bytes(value)
            return value, fmt_json
        else:
            raise ValueFormatException("Only binary and string data supported by RawJSONTranscoder")

    def decode_value(self,
                     value,  # type: bytes
                     flags,  # type: int
                     **kwargs, # type: Dict[str, Any]
                     ) -> Union[str, bytes]:

        impl = kwargs.get('implementation', None)
        format = get_decode_format(flags, impl)

        if format == get_format_flags(TranscoderFormat.FMT_BYTES, impl):
            raise ValueFormatException("Binary format type not supported by RawJSONTranscoder")
        elif format == get_format_flags(TranscoderFormat.FMT_STRING, impl):
            raise ValueFormatException("String format type not supported by RawJSONTranscoder")
        elif format == get_format_flags(TranscoderFormat.FMT_JSON, impl):
            if isinstance(value, str):
                return value.decode('utf-8')
            elif isinstance(value, (bytes, bytearray)):
                if isinstance(value, bytearray):
                    value = bytes(value)
                return value
            else:
                raise ValueFormatException("Only binary and string data supported by RawJSONTranscoder")
        else:
            raise InvalidArgumentException("Unexpected flags value.")


class RawStringTranscoder(Transcoder):

    def encode_value(self,
                     value,  # type: str
                     **kwargs, # type: Dict[str, Any]
                     ) -> Tuple[bytes, int]:

        fmt_utf8 = get_format_flags(TranscoderFormat.FMT_STRING, kwargs.get('implementation', None))

        if isinstance(value, str):
            return value.encode('utf-8'), fmt_utf8
        else:
            raise ValueFormatException("Only string data supported by RawStringTranscoder")

    def decode_value(self,
                     value,  # type: bytes
                     flags,  # type: int
                     **kwargs, # type: Dict[str, Any]
                     ) -> Union[str, bytes]:

        impl = kwargs.get('implementation', None)
        format = get_decode_format(flags, impl)

        if format == get_format_flags(TranscoderFormat.FMT_BYTES, impl):
            raise ValueFormatException("Binary format type not supported by RawStringTranscoder")
        elif format == get_format_flags(TranscoderFormat.FMT_STRING, impl):
            return value.decode('utf-8')
        elif format == get_format_flags(TranscoderFormat.FMT_JSON, impl):
            raise ValueFormatException("JSON format type not supported by RawStringTranscoder")
        else:
            raise InvalidArgumentException("Unexpected flags value.")


class RawBinaryTranscoder(Transcoder):
    def encode_value(self,
                     value,  # type: Union[bytes,bytearray]
                     **kwargs, # type: Dict[str, Any]
                     ) -> Tuple[bytes, int]:

        fmt_bytes = get_format_flags(TranscoderFormat.FMT_BYTES, kwargs.get('implementation', None))

        if isinstance(value, (bytes, bytearray)):
            if isinstance(value, bytearray):
                value = bytes(value)
            return value, fmt_bytes
        else:
            raise ValueFormatException("Only binary data supported by RawBinaryTranscoder")

    def decode_value(self,
                     value,  # type: bytes
                     flags,  # type: int
                     **kwargs, # type: Dict[str, Any]
                     ) -> bytes:

        impl = kwargs.get('implementation', None)
        format = get_decode_format(flags, impl)

        if format == get_format_flags(TranscoderFormat.FMT_BYTES, impl):
            if isinstance(value, bytearray):
                value = bytes(value)
            return value
        elif format == get_format_flags(TranscoderFormat.FMT_STRING, impl):
            raise ValueFormatException("String format type not supported by RawBinaryTranscoder")
        elif format == get_format_flags(TranscoderFormat.FMT_JSON, impl):
            raise ValueFormatException("JSON format type not supported by RawBinaryTranscoder")
        else:
            raise InvalidArgumentException("Unexpected flags value.")


# class LegacyTranscoder(Transcoder):

#     def encode_value(self,
#                      value  # type: Any
#                      ) -> Tuple[bytes, int]:

#         if isinstance(value, str):
#             format = FMT_UTF8
#         elif isinstance(value, (bytes, bytearray)):
#             format = FMT_BYTES
#         elif isinstance(value, (list, tuple, dict, bool, int, float)) or value is None:
#             format = FMT_JSON
#         else:
#             format = FMT_PICKLE

#         if format == FMT_BYTES:
#             if isinstance(value, bytes):
#                 pass
#             elif isinstance(value, bytearray):
#                 value = bytes(value)
#             else:
#                 raise ValueFormatException("Expected bytes")

#             return value, format

#         elif format == FMT_UTF8:
#             return value.encode('utf-8'), format

#         elif format == FMT_PICKLE:
#             return pickle.dumps(value), FMT_PICKLE

#         elif format == FMT_JSON:
#             return json.dumps(value, ensure_ascii=False).encode("utf-8"), FMT_JSON

#         else:
#             raise ValueFormatException("Unrecognized format {}".format(format))

#     def decode_value(self,
#                      value,  # type: bytes
#                      flags  # type: int
#                      ) -> Any:

#         format = get_decode_format(flags)

#         if format == FMT_BYTES:
#             return value
#         elif format == FMT_UTF8:
#             return value.decode("utf-8")
#         elif format == FMT_JSON:
#             try:
#                 return json.loads(value.decode('utf-8'))
#             except Exception:
#                 # if error encountered, assume return bytes
#                 return value
#         elif format == FMT_PICKLE:
#             return pickle.loads(value)  # nosec
#         else:
#             # default to returning bytes
#             return value
