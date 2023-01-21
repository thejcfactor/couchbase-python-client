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

from new_couchbase.classic.core.constants import (FMT_BYTES,
                                 FMT_COMMON_MASK,
                                 FMT_JSON,
                                 FMT_LEGACY_MASK,
                                 FMT_PICKLE,
                                 FMT_UTF8)

UNIFIED_FORMATS = (FMT_JSON, FMT_BYTES, FMT_UTF8, FMT_PICKLE)
LEGACY_FORMATS = tuple([x & FMT_LEGACY_MASK for x in UNIFIED_FORMATS])
COMMON_FORMATS = tuple([x & FMT_COMMON_MASK for x in UNIFIED_FORMATS])

COMMON2UNIFIED = {}
LEGACY2UNIFIED = {}

for fl in UNIFIED_FORMATS:
    COMMON2UNIFIED[fl & FMT_COMMON_MASK] = fl
    LEGACY2UNIFIED[fl & FMT_LEGACY_MASK] = fl


def get_decode_format(flags):
    """
    Returns a tuple of format, recognized
    """
    c_flags = flags & FMT_COMMON_MASK
    l_flags = flags & FMT_LEGACY_MASK

    if c_flags:
        # if unknown format, default to FMT_BYTES
        return COMMON2UNIFIED.get(c_flags, FMT_BYTES)
    else:
        # if unknown format, default to FMT_BYTES
        return LEGACY2UNIFIED.get(l_flags, FMT_BYTES)