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

from protostellar.exceptions import parse_proto_exception

class BlockingWrapper:

    @staticmethod
    def handle_exception(fn):
        @wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            try:
                res = fn(self, *args, **kwargs)
                return res
            except grpc.RpcError as grpc_err:
                ex = parse_proto_exception(grpc_err)
                raise ex
            except Exception as ex:
                raise ex

        return wrapped_fn