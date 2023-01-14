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

from datetime import timedelta
from typing import (Any,
                    Dict,
                    List,
                    TypeVar,
                    Union)
from urllib.parse import quote

from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.exceptions import InvalidArgumentException

JSONType = Union[str, int, float, bool,
                 None, Dict[str, Any], List[Any]]


def is_null_or_empty(
    value  # type: str
) -> bool:
    return not (value and not value.isspace())


def to_form_str(params  # type: Dict[str, Any]
                ):
    encoded_params = []
    for k, v in params.items():
        encoded_params.append('{0}={1}'.format(quote(k), quote(str(v))))

    return '&'.join(encoded_params)


def identity(x  # type: Any
             ) -> Any:
    return x


def timedelta_as_microseconds(
    duration,  # type: timedelta
) -> int:
    if duration and not isinstance(duration, timedelta):
        raise InvalidArgumentException(
            message="Expected timedelta instead of {}".format(duration)
        )
    return int(duration.total_seconds() * 1e6 if duration else 0)


def to_microseconds(
    timeout  # type: Union[timedelta, float, int]
) -> int:
    if timeout and not isinstance(timeout, (timedelta, float, int)):
        raise InvalidArgumentException(message=("Excepted timeout to be of type "
                                                f"Union[timedelta, float, int] instead of {timeout}"))
    if not timeout:
        total_us = 0
    elif isinstance(timeout, timedelta):
        total_us = int(timeout.total_seconds() * 1e6)
    else:
        total_us = int(timeout * 1e6)

    return total_us


def validate_bool(value  # type: bool
                  ) -> bool:
    if not isinstance(value, bool):
        raise InvalidArgumentException(message='Expected value to be of type bool.')
    return value

def validate_int(value  # type: int
                 ) -> int:
    if not isinstance(value, int):
        raise InvalidArgumentException(message='Expected value to be of type int.')
    return value

def validate_projections(projections # type: List[str]
    ) -> List[str]:
    if not (isinstance(projections, list) and all(map(lambda p: isinstance(p, str), projections))):
        raise InvalidArgumentException("Projects must be List[str].")

    if len(projections) > 16:
        raise InvalidArgumentException(
            f"Maximum of 16 projects allowed. Provided {len(projections)}"
        )

    return projections

def validate_serializer(value # type: SerializerInterface
        ) -> SerializerInterface:
    if not issubclass(value, SerializerInterface):
        raise InvalidArgumentException(message='Expected value to implement SerializerInterface.')
    return value

def validate_str(value  # type: str
                 ) -> int:
    if not isinstance(value, str):
        raise InvalidArgumentException(message='Expected value to be of type str.')
    return value

def validate_transcoder(value # type: TranscoderInterface
        ) -> TranscoderInterface:
    if not issubclass(value, TranscoderInterface):
        raise InvalidArgumentException(message='Expected value to implement TranscoderInterface.')
    return value