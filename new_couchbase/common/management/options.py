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

from enum import Enum

from typing import Any, Dict, Iterable, List, Optional, Union, overload, TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import timedelta

class BucketMgmtOptionTypes(Enum):
    Create = 'CreateOptions'
    Drop = 'DropOptions'
    Flush = 'FlushOptions'
    Get = 'GetOptions'
    GetAll = 'GetAllOptions'
    Update = 'UpdateOptions'

class CollectionMgmtOptionTypes(Enum):
    CollectionMgmt = 'CollectionMgmtOptions'


class OptionsBase(dict):
    @overload
    def __init__(self,
                 timeout=None,       # type: Optional[timedelta]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    def timeout(self,
                timeout,  # type: timedelta
                ) -> OptionsBase:
        self['timeout'] = timeout
        return self

"""

Python SDK Bucket Management Operation Options Base Classes

"""

class CreateBucketOptionsBase(OptionsBase):
    pass

class DropBucketOptionsBase(OptionsBase):
    pass

class FlushBucketOptionsBase(OptionsBase):
    pass

class GetAllBucketOptionsBase(OptionsBase):
    pass

class GetBucketOptionsBase(OptionsBase):
    pass

class UpdateBucketOptionsBase(OptionsBase):
    pass