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

import copy
from typing import Any, Dict

from new_couchbase.protostellar._utils import to_seconds
from new_couchbase.common.management.options import BucketMgmtOptionTypes
from new_couchbase.exceptions import InvalidArgumentException

class ValidBucketMgmtOptions:

    _VALID_OPTS = {
        'timeout': {'timeout': to_seconds},
    }

    @staticmethod
    def get_valid_options(opt_type # type: BucketMgmtOptionTypes
        ) -> Dict[str, Any]:

        opt_types = [e.value for e in BucketMgmtOptionTypes]

        if opt_type.value in opt_types:
            valid_opts = copy.copy(ValidBucketMgmtOptions._VALID_OPTS)
        else:
            raise InvalidArgumentException(f"Invalid option type: {opt_type}")
        
        return valid_opts