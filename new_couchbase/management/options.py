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

from new_couchbase.common.management.options import (CreateBucketOptionsBase,
                                                     DropBucketOptionsBase,
                                                     FlushBucketOptionsBase,
                                                     GetAllBucketOptionsBase,
                                                     GetBucketOptionsBase,
                                                     UpdateBucketOptionsBase)


class MgmtOptionTypes(Enum):
    BucketMgmt = 'BucketMgmtOptions'
    CollectionMgmt = 'CollectionMgmtOptions'


"""

Python SDK Bucket Management Operation Options Classes

"""

class CreateBucketOptions(CreateBucketOptionsBase):
    """Available options for a :class:`~couchbase.management.buckets.BucketManager`'s create bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """

class DropBucketOptions(DropBucketOptionsBase):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s drop bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

class FlushBucketOptions(FlushBucketOptionsBase):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s flush bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

class GetAllBucketOptions(GetAllBucketOptionsBase):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s get all buckets operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """


class GetBucketOptions(GetBucketOptionsBase):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s get bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """

class UpdateBucketOptions(UpdateBucketOptionsBase):
    """Available options to for a :class:`~couchbase.management.buckets.BucketManager`'s update bucket operation.

    .. note::
        All management options should be imported from ``couchbase.management.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            management operation timeout.
    """