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

from couchbase.exceptions import InvalidArgumentException

from couchbase.subdocument import Spec  # noqa: F401
from couchbase.subdocument import StoreSemantics  # noqa: F401
from couchbase.subdocument import SubDocOp  # noqa: F401
from couchbase.subdocument import exists  # noqa: F401
from couchbase.subdocument import get  # noqa: F401
from couchbase.subdocument import count  # noqa: F401

from protostellar.proto.couchbase.kv import v1_pb2

def to_protostellar_lookup_in_spec(spec # type: Spec
    ) -> v1_pb2.LookupInRequest.Spec:
    (op, path, xattr) = spec
    params = {
        'path': path,
    }
    if op == SubDocOp.EXISTS:
        params['operation'] = v1_pb2.LookupInRequest.Spec.Operation.EXISTS
        params['flags'] = v1_pb2.LookupInRequest.Spec.Flags(xattr=xattr)
    elif op == SubDocOp.GET:
        params['operation'] = v1_pb2.LookupInRequest.Spec.Operation.GET
        params['flags'] = v1_pb2.LookupInRequest.Spec.Flags(xattr=xattr)
    elif op == SubDocOp.GET_COUNT:
        params['operation'] = v1_pb2.LookupInRequest.Spec.Operation.COUNT
        params['flags'] = v1_pb2.LookupInRequest.Spec.Flags(xattr=xattr)
    else:
        raise InvalidArgumentException(f'Unable to determine lookup-in spec: {spec}')

    return v1_pb2.LookupInRequest.Spec(**params)

def to_protostellar_mutate_in_spec(spec # type: Spec
    ) -> v1_pb2.MutateInRequest.Spec:
    value = None
    if len(spec) == 6:
        op, path, create_path, xattr, macros, value = spec
    else:
        op, path, create_path, xattr, macros = spec
    params = {
        'path': path,
    }
    if op == SubDocOp.DICT_ADD:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.INSERT
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.DICT_UPSERT:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.UPSERT
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.REPLACE:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.REPLACE
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.REMOVE:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.REMOVE
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.ARRAY_PUSH_LAST:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.ARRAY_APPEND
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.ARRAY_PUSH_FIRST:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.ARRAY_PREPEND
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.ARRAY_INSERT:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.ARRAY_INSERT
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.ARRAY_ADD_UNIQUE:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.ARRAY_ADD_UNIQUE
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    elif op == SubDocOp.COUNTER:
        params['operation'] = v1_pb2.MutateInRequest.Spec.Operation.COUNTER
        params['flags'] = v1_pb2.MutateInRequest.Spec.Flags(create_path=create_path,
                                                            xattr=xattr)
    else:
        raise InvalidArgumentException(f'Unable to determine lookup-in spec: {spec}')

    if value:
        params['content'] = json.dumps(value, ensure_ascii=False).encode('utf-8')

    return v1_pb2.MutateInRequest.Spec(**params)

    

    