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

import json

from typing import TYPE_CHECKING

from new_couchbase.exceptions import InvalidArgumentException

from new_couchbase.subdocument import Spec  # noqa: F401
from new_couchbase.subdocument import StoreSemantics  # noqa: F401
from new_couchbase.subdocument import SubDocOp
from new_couchbase.subdocument import exists  # noqa: F401
from new_couchbase.subdocument import get  # noqa: F401
from new_couchbase.subdocument import count  # noqa: F401

from new_couchbase.protostellar.proto.couchbase.kv.v1 import kv_pb2

if TYPE_CHECKING:
    from new_couchbase.transcoder import Transcoder

def to_protostellar_lookup_in_spec(spec # type: Spec
    ) -> kv_pb2.LookupInRequest.Spec:
    (op, path, xattr) = spec
    params = {
        'path': path,
    }
    if op == SubDocOp.EXISTS:
        params['operation'] = kv_pb2.LookupInRequest.Spec.Operation.OPERATION_EXISTS
        params['flags'] = kv_pb2.LookupInRequest.Spec.Flags(xattr=xattr)
    elif op == SubDocOp.GET:
        params['operation'] = kv_pb2.LookupInRequest.Spec.Operation.OPERATION_GET
        params['flags'] = kv_pb2.LookupInRequest.Spec.Flags(xattr=xattr)
    elif op == SubDocOp.GET_COUNT:
        params['operation'] = kv_pb2.LookupInRequest.Spec.Operation.OPERATION_COUNT
        params['flags'] = kv_pb2.LookupInRequest.Spec.Flags(xattr=xattr)
    else:
        raise InvalidArgumentException(f'Unable to determine lookup-in spec: {spec}')

    return kv_pb2.LookupInRequest.Spec(**params)

def to_protostellar_mutate_in_spec(spec, # type: Spec
                                    transcoder, # type: Transcoder
    ) -> kv_pb2.MutateInRequest.Spec:
    value = None
    if len(spec) == 6:
        op, path, create_path, xattr, macros, value = spec
    else:
        op, path, create_path, xattr, macros = spec
    params = {
        'path': path,
    }
    if op == SubDocOp.DICT_ADD:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_INSERT
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.DICT_UPSERT:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_UPSERT
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.REPLACE:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_REPLACE
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.REMOVE:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_REMOVE
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.ARRAY_PUSH_LAST:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_ARRAY_APPEND
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.ARRAY_PUSH_FIRST:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_ARRAY_PREPEND
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.ARRAY_INSERT:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_ARRAY_INSERT
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.ARRAY_ADD_UNIQUE:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_ARRAY_ADD_UNIQUE
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.COUNTER:
        params['operation'] = kv_pb2.MutateInRequest.Spec.Operation.OPERATION_COUNTER
        params['flags'] = kv_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    else:
        raise InvalidArgumentException(f'Unable to determine mutate-in spec: {spec}')

    multi_ops = [SubDocOp.ARRAY_PUSH_FIRST,
                 SubDocOp.ARRAY_PUSH_LAST,
                 SubDocOp.ARRAY_ADD_UNIQUE,
                 SubDocOp.ARRAY_INSERT]

    if value:
        if op in multi_ops:
            tmp_value = json.dumps(value, ensure_ascii=False)
            # need to remove the brackets from the array
            params['content'] = tmp_value[1:len(tmp_value)-1].encode('utf-8')
        else:
            content, _ = transcoder.encode_value(value)
            params['content'] = content

    return kv_pb2.MutateInRequest.Spec(**params)

    

    