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

from copy import copy
from functools import wraps

import grpc
from grpc import StatusCode

from new_couchbase.common.management import ManagementType
from new_couchbase.exceptions import CouchbaseException
from new_couchbase.protostellar.exceptions import ErrorMapper

"""

Bucket mgmt helpers for parsing returned results

"""


def get_bucket_settings(res, return_cls):
    pass
    # raw_settings = res.raw_result.get('bucket_settings', None)
    # bucket_settings = None
    # if raw_settings:
    #     bucket_settings = return_cls(**BucketManagerCore.bucket_settings_from_server(raw_settings))

    # return bucket_settings


def get_all_bucket_settings(res, return_cls):
    pass
    # raw_buckets = res.raw_result.get('buckets', None)
    # buckets = []
    # if raw_buckets:
    #     for b in raw_buckets:
    #         bucket_settings = return_cls(**BucketManagerCore.bucket_settings_from_server(b))
    #         buckets.append(bucket_settings)

    # return buckets

def handle_bucket_mgmt_response(ret, fn_name, return_cls):
    if fn_name == 'get_all_buckets':
        from .buckets import BucketManager
        buckets = []
        for bucket in ret.response.buckets:
            buckets.append(BucketManager.bucket_settings_from_protostellar(bucket))
        retval = buckets
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
                    if return_cls is None:
                        return None
                    elif return_cls is True:
                        retval = ret
                    else:
                        if mgmt_type == ManagementType.BucketMgmt:
                            retval = handle_bucket_mgmt_response(ret, fn.__name__, return_cls)

                    return retval
                except grpc.RpcError as grpc_err:
                    metadata = [(k,v) for k, v in grpc_err.initial_metadata()]
                    metadata.extend([(k,v) for k, v in grpc_err.trailing_metadata()])
                    ex = ErrorMapper.build_exception(grpc_err, metadata)
                    raise ex
                except CouchbaseException as e:
                    raise e
                except Exception as ex:
                    raise

            return wrapped_fn
        return decorator