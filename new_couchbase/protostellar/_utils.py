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

from datetime import datetime, timedelta
from time import time
from typing import Union

from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration

from new_couchbase.exceptions import InvalidArgumentException


def timedelta_as_timestamp(
    duration,  # type: timedelta
) -> Timestamp:
    if not isinstance(duration, timedelta):
        raise InvalidArgumentException(f'Expected timedelta instead of {duration}')

    # PYCBC-1177 remove deprecated heuristic from PYCBC-948:
    seconds = int(duration.total_seconds())
    if seconds < 0:
        raise InvalidArgumentException(f'Expected expiry seconds of zero (for no expiry) or greater, got {seconds}.')

    ts = Timestamp()
    ts.FromSeconds(int(time()) + seconds)
    # another possible route:
    #   then = datetime.utcnow().replace(tzinfo=timezone.utc) + duration
    #   ts.FromDatetime(then)
    return ts

def timedelta_as_seconds(
    duration,  # type: timedelta
) -> int:
    if not isinstance(duration, timedelta):
        raise InvalidArgumentException(f'Expected timedelta instead of {duration}')

    # PYCBC-1177 remove deprecated heuristic from PYCBC-948:
    seconds = int(duration.total_seconds())
    if seconds < 0:
        raise InvalidArgumentException(f'Expected expiry seconds of zero (for no expiry) or greater, got {seconds}.')

    return seconds


def timedelta_as_duration(
    duration,  # type: timedelta
) -> int:
    if not isinstance(duration, timedelta):
        raise InvalidArgumentException(f'Expected timedelta instead of {duration}')

    seconds = int(duration.total_seconds())
    if seconds < 0:
        raise InvalidArgumentException(f'Expected expiry seconds of zero (for no expiry) or greater, got {seconds}.')

    d = Duration()
    d.FromTimedelta(duration)
    return d

def timestamp_as_datetime(
    timestamp,  # type: Timestamp
) -> datetime:
    if not isinstance(timestamp, Timestamp):
        raise InvalidArgumentException(f'Expected Timestamp instead of {timestamp}')

    return timestamp.ToDatetime()

def to_seconds(
    timeout  # type: Union[timedelta, float, int]
) -> int:
    if timeout and not isinstance(timeout, (timedelta, float, int)):
        raise InvalidArgumentException(message=("Excepted timeout to be of type "
                                                f"Union[timedelta, float, int] instead of {timeout}"))
    if not timeout:
        total_secs = 0
    elif isinstance(timeout, timedelta):
        total_secs = timeout.total_seconds()
    else:
        total_secs = timeout

    return total_secs