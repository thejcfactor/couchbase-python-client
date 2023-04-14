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
                    Callable,
                    Dict,
                    List,
                    Optional,
                    TypeVar,
                    Union,
                    TYPE_CHECKING)
from urllib.parse import quote

from new_couchbase.common.options import DeltaValueBase, SignedInt64Base
from new_couchbase.exceptions import InvalidArgumentException
from new_couchbase.serializer import Serializer
from new_couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from enum import Enum

JSONType = Union[str, int, float, bool,
                 None, Dict[str, Any], List[Any]]


def enum_to_str(value, # type: Enum
                enum, # type: Enum
                conversion_fn=None, # type: Optional[Callable]
                ) -> str:
    if isinstance(value, str) and value in map(lambda x: x.value, enum):
        # TODO: use warning?
        # warn("Using deprecated string parameter {}".format(value))
        return value
    if not isinstance(value, enum):
        raise InvalidArgumentException(f"Argument must be of type {enum} but got {value}")
    if conversion_fn:
        try:
            return conversion_fn(value)
        except Exception:
            raise InvalidArgumentException(f"Unable to convert enum value {value} to str.")

    return value.value

def identity(x  # type: Any
             ) -> Any:
    return x

def is_null_or_empty(
    value  # type: str
) -> bool:
    return not (value and not value.isspace())

def str_to_enum(value, # type: str
                enum, # type: Enum
                conversion_fn=None, # type: Optional[Callable]
                ) -> str:
    if not isinstance(value, str):
        raise InvalidArgumentException(f"Argument must be of type str but got {type(value)}.")
    try:
        if conversion_fn:
            return conversion_fn(value)
        return enum(value)
    except Exception:
        raise InvalidArgumentException(f"Unable to convert {value} to enum of type {enum}.")
    

def timedelta_as_microseconds(
    duration,  # type: timedelta
) -> int:
    if duration and not isinstance(duration, timedelta):
        raise InvalidArgumentException(
            message="Expected timedelta instead of {}".format(duration)
        )
    return int(duration.total_seconds() * 1e6 if duration else 0)

def to_form_str(params  # type: Dict[str, Any]
                ):
    encoded_params = []
    for k, v in params.items():
        encoded_params.append('{0}={1}'.format(quote(k), quote(str(v))))

    return '&'.join(encoded_params)

def to_microseconds(
    value  # type: Union[timedelta, float, int]
) -> int:
    if value and not isinstance(value, (timedelta, float, int)):
        raise InvalidArgumentException(message=("Excepted value to be of type "
                                                f"Union[timedelta, float, int] instead of {value}"))
    if not value:
        total_us = 0
    elif isinstance(value, timedelta):
        total_us = int(value.total_seconds() * 1e6)
    else:
        total_us = int(value * 1e6)

    return total_us

def to_seconds(
    value  # type: Union[timedelta, float, int]
) -> int:
    if value and not isinstance(value, (timedelta, float, int)):
        raise InvalidArgumentException(message=("Excepted value to be of type "
                                                f"Union[timedelta, float, int] instead of {value}"))
    if not value:
        total_secs = 0
    elif isinstance(value, timedelta):
        total_secs = int(value.total_seconds())
    else:
        total_secs = int(value)

    return total_secs

def validate_bool(value  # type: bool
                  ) -> bool:
    if not isinstance(value, bool):
        raise InvalidArgumentException(message='Expected value to be of type bool.')
    return value

def validate_binary_counter_delta(value  # type: Optional[DeltaValueBase]
                  ) -> DeltaValueBase:
    if not DeltaValueBase.is_valid(value):
        raise InvalidArgumentException(message='Expected value to be of type DeltaValue.')
    return value

def validate_binary_counter_initial(value  # type: Optional[SignedInt64Base]
                  ) -> SignedInt64Base:
    if not SignedInt64Base.is_valid(value):
        raise InvalidArgumentException(message='Expected value to be of type SignedInt64.')
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

def validate_serializer(value # type: Serializer
        ) -> Serializer:
    if not issubclass(value.__class__, Serializer):
        raise InvalidArgumentException(message='Expected value to implement Serializer.')
    return value

def validate_str(value  # type: str
                 ) -> int:
    if not isinstance(value, str):
        raise InvalidArgumentException(message='Expected value to be of type str.')
    return value

def validate_transcoder(value # type: Transcoder
        ) -> Transcoder:
    if not issubclass(value.__class__, Transcoder):
        raise InvalidArgumentException(message='Expected value to implement Transcoder.')
    return value