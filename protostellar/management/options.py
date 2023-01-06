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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional,
                    overload)

from couchbase._utils import (timedelta_as_microseconds,)

if TYPE_CHECKING:
    from datetime import timedelta


class CreateBucketOptions(dict):
    """Available options for a :class:`~protostellar.management.buckets.BucketManager`'s create bucket operation.

    .. note::
        All management options should be imported from ``protostellar.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """

    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetBucketOptions(dict):
    """Available options for a :class:`~protostellar.management.buckets.BucketManager`'s get bucket operation.

    .. note::
        All management options should be imported from ``protostellar.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """

    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class GetAllBucketOptions(dict):
    """Available options for a :class:`~protostellar.management.buckets.BucketManager`'s get all buckets operation.

    .. note::
        All management options should be imported from ``protostellar.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """

    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)        