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

from typing import TYPE_CHECKING, Any, Dict, List, Tuple


from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api.scope import ScopeInterface
from new_couchbase.api.transcoder import TranscoderInterface

from new_couchbase.api import ApiImplementation

from new_couchbase.impl.protostellar.result import GetResult, MutationResult, ProtostellarResponse

from new_couchbase.common.options import OptionTypes, parse_options
from new_couchbase.impl.protostellar.options import ValidKeyValueOptions

from new_couchbase.impl.protostellar import kv_grpc_module as kv

from new_couchbase.impl.protostellar.proto.couchbase.kv import v1_pb2
from new_couchbase.impl.protostellar.wrappers import BlockingWrapper


if TYPE_CHECKING:
    from new_couchbase.options import GetOptions, UpsertOptions
    from new_couchbase.common._utils import JSONType

class Collection(CollectionInterface):
    def __init__(self, 
                scope, # type: ScopeInterface
                name # type str
                ):
        # if not scope:
        #     raise InvalidArgumentException(message="Collection must be given a scope")
        # if not scope.connection:
        #     raise RuntimeError("No connection provided")
        self._scope = scope
        self._collection_name = name
        self._connection = scope.connection
        self._kv = kv.KvStub(self._connection)

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.PROTOSTELLAR

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._connection

    @property
    def default_transcoder(self) -> TranscoderInterface:
        return self._scope.default_transcoder

    @property
    def metadata(self) -> List[Tuple[str]]:
        """
        **INTERNAL**
        """
        return self._scope.metadata

    @property
    def name(self) -> str:
        return self._collection_name

    def _get_namespace_args(self) -> Dict[str, str]:
        return {
            'bucket_name': self._scope.bucket_name,
            'scope_name': self._scope.name,
            'collection_name': self.name,
        }

    @BlockingWrapper.decode_read_op(GetResult)
    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Dict[str, Any]
            ) -> GetResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Get), kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
            
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req = v1_pb2.GetRequest(**req_args)
        response, call = self._kv.Get.with_call(req, metadata=self.metadata)
        metadata = [(k,v) for k, v in call.trailing_metadata()]
        return ProtostellarResponse(response, metadata, key, transcoder)

    @BlockingWrapper.decode_mutation_op(MutationResult)
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Upsert), kwargs, *opts)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        content, content_type = transcoder.encode_value(value)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['content'] = content
        req_args['content_type'] = content_type
        req = v1_pb2.UpsertRequest(**req_args)
        response, call = self._kv.Upsert.with_call(req, metadata=self.metadata)
        metadata = [(k,v) for k, v in call.trailing_metadata()]
        return ProtostellarResponse(response, metadata, key, None)

    @staticmethod
    def default_name():
        return "_default"