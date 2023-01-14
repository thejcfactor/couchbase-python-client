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
from google.protobuf.timestamp_pb2 import Timestamp

from new_couchbase.exceptions import (InternalSDKException, 
                                      CouchbaseException, 
                                      ServiceUnavailableException, 
                                      UnAmbiguousTimeoutException)

from new_couchbase.api import ApiImplementation
from new_couchbase.impl.protostellar._utils import timestamp_as_datetime
from new_couchbase.impl.protostellar.exceptions import ErrorMapper



BASE_TIMESTAMP = Timestamp()

class BlockingWrapper:
    @classmethod  # noqa: C901
    def decode_read_op(cls, return_cls):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    # response is a Tuple(response, metatdata, key, transcoder)
                    ret = fn(self, *args, **kwargs)
                    ret_val = {
                        'key': ret[2],
                    }
                    if hasattr(ret[0], 'cas'):
                        ret_val['cas'] = ret[0].cas
                    if hasattr(ret[0], 'content_type'):
                        value = ret[3].decode_value(ret[0].content, 
                                                    ret[0].content_type,
                                                    implementation=ApiImplementation.PROTOSTELLAR)
                        ret_val['value'] = value
                    if hasattr(ret[0], 'expiry') and ret[0].expiry != BASE_TIMESTAMP:
                        ret_val['expiry'] = timestamp_as_datetime(ret[0].expiry)
                    return return_cls(ret_val)
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

    @classmethod  # noqa: C901
    def decode_mutation_op(cls, return_cls):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    # response is a Tuple(response, metatdata, key, None)
                    ret = fn(self, *args, **kwargs)
                    ret_val = {
                        'cas': ret[0].cas,
                        'key': ret[2],
                        'mutation_token': ret[0].mutation_token,
                    }
                    return return_cls(ret_val)
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