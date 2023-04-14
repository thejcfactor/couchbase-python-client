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

from functools import wraps
from typing import Any, Dict

from new_couchbase.common.management import ManagementType
from new_couchbase.common._utils import str_to_enum
from new_couchbase.exceptions import CouchbaseException, HTTPException
from new_couchbase.classic.exceptions import ErrorMapper, ExceptionMap, PYCBC_ERROR_MAP
from new_couchbase.classic.exceptions import exception as BaseCouchbaseException

from .buckets import BucketManagerCore




"""

Bucket mgmt helpers for parsing returned results

"""


def get_bucket_settings(res, return_cls):
    raw_settings = res.raw_result.get('bucket_settings', None)
    bucket_settings = None
    if raw_settings:
        bucket_settings = return_cls(**BucketManagerCore.bucket_settings_from_server(raw_settings))

    return bucket_settings


def get_all_bucket_settings(res, return_cls):
    raw_buckets = res.raw_result.get('buckets', None)
    buckets = []
    if raw_buckets:
        for b in raw_buckets:
            bucket_settings = return_cls(**BucketManagerCore.bucket_settings_from_server(b))
            buckets.append(bucket_settings)

    return buckets



"""

General mgmt helpers for parsing returned results

"""

def handle_mgmt_exception(exc, mgmt_type, error_map):
    raise ErrorMapper.build_exception(exc, mapping=error_map)


def handle_bucket_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_bucket':
        retval = get_bucket_settings(ret, return_cls)
    elif fn_name == 'get_all_buckets':
        retval = get_all_bucket_settings(ret, return_cls)
    else:
        retval = return_cls(ret)

    return retval


class BlockingMgmtWrapper:

    @classmethod  # noqa: C901
    def block(cls, return_cls, mgmt_type, error_map):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    ret = fn(self, *args, **kwargs)
                    if isinstance(ret, BaseCouchbaseException):
                        handle_mgmt_exception(ret, mgmt_type, error_map)
                    if return_cls is None:
                        return None
                    elif return_cls is True:
                        retval = ret
                    else:
                        if mgmt_type == ManagementType.BucketMgmt:
                            retval = handle_bucket_mgmt_response(ret, fn.__name__, return_cls)
                        # elif mgmt_type == ManagementType.CollectionMgmt:
                        #     retval = handle_collection_mgmt_response(ret, fn.__name__, return_cls)
                        # elif mgmt_type == ManagementType.UserMgmt:
                        #     retval = handle_user_mgmt_response(ret, fn.__name__, return_cls)
                        # elif mgmt_type == ManagementType.QueryIndexMgmt:
                        #     retval = handle_query_index_mgmt_response(ret, fn.__name__, return_cls)
                        # elif mgmt_type == ManagementType.AnalyticsIndexMgmt:
                        #     retval = handle_analytics_index_mgmt_response(ret, fn.__name__, return_cls)
                        # elif mgmt_type == ManagementType.SearchIndexMgmt:
                        #     retval = handle_search_index_mgmt_response(ret, fn.__name__, return_cls)
                        # elif mgmt_type == ManagementType.ViewIndexMgmt:
                        #     retval = handle_view_index_mgmt_response(ret, fn.__name__, return_cls)
                        # elif mgmt_type == ManagementType.EventingFunctionMgmt:
                        #     retval = handle_eventing_function_mgmt_response(ret, fn.__name__, return_cls)
                        else:
                            retval = None
                    return retval
                except HTTPException as e:
                    raise e
                except CouchbaseException as e:
                    raise e
                except Exception as ex:
                    if isinstance(ex, (TypeError, ValueError)):
                        raise ex
                    exc_cls = PYCBC_ERROR_MAP.get(ExceptionMap.InternalSDKException.value, CouchbaseException)
                    excptn = exc_cls(message=str(ex))
                    raise excptn

            return wrapped_fn
        return decorator