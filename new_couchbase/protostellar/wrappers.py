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
from google.protobuf.timestamp_pb2 import Timestamp

from new_couchbase.exceptions import (InternalSDKException, 
                                      CouchbaseException, 
                                      ServiceUnavailableException, 
                                      UnAmbiguousTimeoutException)

from new_couchbase.api import ApiImplementation
from new_couchbase.protostellar._utils import timestamp_as_datetime
from new_couchbase.protostellar.exceptions import ErrorMapper
from new_couchbase.protostellar.proto.couchbase.kv import v1_pb2



BASE_TIMESTAMP = Timestamp()

class BlockingWrapper:
    @classmethod  # noqa: C901
    def decode_read_op(cls, return_cls):  # noqa: C901
        def decorator(fn):
            @wraps(fn)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    # response is a named_tuple(response, call, key, transcoder)
                    ret = fn(self, *args, **kwargs)
                    ret_val = {
                        'key': ret.key,
                    }
                    if hasattr(ret.response, 'cas'):
                        ret_val['cas'] = ret.response.cas
                    if hasattr(ret.response, 'content_type'):
                        value = ret.transcoder.decode_value(ret.response.content, 
                                                    ret.response.content_type,
                                                    implementation=ApiImplementation.PROTOSTELLAR)
                        ret_val['value'] = value
                    elif hasattr(ret.response, 'content'): # increment/decrement
                        ret_val['content'] = ret.response.content
                    if hasattr(ret.response, 'expiry') and ret.response.expiry != BASE_TIMESTAMP:
                        ret_val['expiry'] = timestamp_as_datetime(ret.response.expiry)
                    if hasattr(ret.response, 'result'):
                        ret_val['result'] = ret.response.result
                        
                    # lookup-in result
                    if hasattr(ret.response, 'specs'):
                        ret_val['value'] = []
                        for spec in ret.response.specs:
                            if spec.status.code == StatusCode.OK.value[0]:
                                decoded = ret.transcoder.decode_value(spec.content,
                                                                    v1_pb2.JSON,
                                                                    implementation=ApiImplementation.PROTOSTELLAR)
                            else:
                                decoded = None
                            ret_val['value'].append({
                                'value': decoded,
                                'status': spec.status,
                            })
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
                    if return_cls is None:
                        return
                    ret_val = {
                        'cas': ret.response.cas,
                        'key': ret.key,
                        'mutation_token': ret.response.mutation_token,
                    }
                    # mutate-in result
                    if hasattr(ret.response, 'specs'):
                        ret_val['value'] = []
                        for spec in ret.response.specs:
                            decoded = ret.transcoder.decode_value(spec.content,
                                                                v1_pb2.JSON,
                                                                implementation=ApiImplementation.PROTOSTELLAR)
                            ret_val['value'].append({
                                'value': decoded,
                            })

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