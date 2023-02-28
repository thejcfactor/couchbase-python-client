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

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Tuple

from new_couchbase.scope import Scope
from new_couchbase.transcoder import JSONTranscoder, Transcoder

from new_couchbase.api import ApiImplementation

from new_couchbase.protostellar.result import (CounterResult,
                                               ExistsResult,
                                               GetReplicaResult,
                                               GetResult,
                                               LookupInResult,
                                               MutateInResult,
                                               MutationResult,
                                               ProtostellarResponse)

from new_couchbase.common.options import OptionTypes, parse_options
from new_couchbase.exceptions import InvalidArgumentException, FeatureUnavailableException
from new_couchbase.protostellar._utils import timedelta_as_timestamp
from new_couchbase.protostellar.binary_collection import BinaryCollection
from new_couchbase.protostellar.options import ValidKeyValueOptions
from new_couchbase.protostellar.subdocument import to_protostellar_lookup_in_spec, to_protostellar_mutate_in_spec

from new_couchbase.protostellar import kv_grpc_module as kv

from new_couchbase.protostellar.proto.couchbase.kv import v1_pb2
from new_couchbase.protostellar.wrappers import BlockingWrapper


if TYPE_CHECKING:
    from datetime import timedelta

    from new_couchbase.protostellar.scope import Scope
    from new_couchbase.options import (ExistsOptions,
                                       GetOptions,
                                       GetAllReplicasOptions,
                                       GetAndLockOptions,
                                       GetAndTouchOptions,
                                       GetAnyReplicaOptions,
                                       InsertOptions,
                                       LookupInOptions,
                                       MutateInOptions,
                                       RemoveOptions,
                                       ReplaceOptions,
                                       TouchOptions,
                                       UnlockOptions,
                                       UpsertOptions)
    from new_couchbase.common._utils import JSONType
    from new_couchbase.subdocument import Spec

class Collection:
    def __init__(self, 
                scope, # type: Scope
                collection_name # type: str
                ):
        # if not scope:
        #     raise InvalidArgumentException(message="Collection must be given a scope")
        # if not scope.connection:
        #     raise RuntimeError("No connection provided")
        self._scope = scope
        self._collection_name = collection_name
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
    def default_transcoder(self) -> Transcoder:
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

    @BlockingWrapper.decode_mutation_op(MutationResult)
    def _append(self,
               key,  # type: str
               value,  # type: JSONType
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        call_args = self._get_call_args(**kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key

        if isinstance(value, str):
            value = value.encode('utf-8')
        elif isinstance(value, bytearray):
            value = bytes(value)
        if not isinstance(value, bytes):
            raise ValueError("The value provided must of type str, bytes or bytearray.")        
        req_args['content'] = value
        if 'cas' in kwargs:
            req_args['cas'] = kwargs.get('cas')
        req = v1_pb2.AppendRequest(**req_args)
        response, call = self._kv.Append.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)

    @BlockingWrapper.decode_read_op(CounterResult)
    def _decrement(self,
                   key,  # type: str               
                   **kwargs,  # type: Dict[str, Any]
                   ) -> CounterResult:
        call_args = self._get_call_args(**kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['initial'] = int(kwargs['initial'])
        req_args['delta'] = int(kwargs['delta'])
        if 'expiry' in kwargs:
            req_args['expiry'] = kwargs.get('expiry')
        req = v1_pb2.DecrementRequest(**req_args)
        response, call = self._kv.Decrement.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)

    def _get_call_args(self, 
                      **options, # type: Dict[str, Any]
                      ) -> Dict[str, Any]:
        timeout = options.pop('timeout', None)
        call_args = {
            'metadata': self.metadata
        }
        if timeout:
            call_args['timeout'] = timeout

        return call_args

    def _get_namespace_args(self) -> Dict[str, str]:
        return {
            'bucket_name': self._scope.bucket_name,
            'scope_name': self._scope.name,
            'collection_name': self.name,
        }
    
    @BlockingWrapper.decode_read_op(CounterResult)
    def _increment(self,
                   key,  # type: str
                   **kwargs,  # type: Dict[str, Any]
                   ) -> CounterResult:
        call_args = self._get_call_args(**kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['initial'] = int(kwargs['initial'])
        req_args['delta'] = int(kwargs['delta'])
        if 'expiry' in kwargs:
            req_args['expiry'] = kwargs.get('expiry')
        req = v1_pb2.IncrementRequest(**req_args)
        response, call = self._kv.Increment.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)
    
    @BlockingWrapper.decode_mutation_op(MutationResult)
    def _prepend(self,
               key,  # type: str
               value,  # type: JSONType
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        call_args = self._get_call_args(**kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key

        if isinstance(value, str):
            value = value.encode('utf-8')
        elif isinstance(value, bytearray):
            value = bytes(value)
        if not isinstance(value, bytes):
            raise ValueError("The value provided must of type str, bytes or bytearray.")        
        req_args['content'] = value
        if 'cas' in kwargs:
            req_args['cas'] = kwargs.get('cas')
        req = v1_pb2.PrependRequest(**req_args)
        response, call = self._kv.Prepend.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)

    def binary(self) -> BinaryCollection:
        return BinaryCollection(self)

    @BlockingWrapper.decode_read_op(ExistsResult)
    def exists(self,
               key,  # type: str
               *opts,  # type: ExistsOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> ExistsResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Exists), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder


        req_args = self._get_namespace_args()
        req_args['key'] = key
        req = v1_pb2.ExistsRequest(**req_args)
        response, call = self._kv.Exists.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, transcoder)

    @BlockingWrapper.decode_read_op(GetResult)
    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Dict[str, Any]
            ) -> GetResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Get), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder

        req_args = self._get_namespace_args()
        req_args['key'] = key
        req = v1_pb2.GetRequest(**req_args)
        response, call = self._kv.Get.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, transcoder)

    @BlockingWrapper.decode_read_op(GetReplicaResult)
    def get_all_replicas(self,
                     key,  # type: str
                     *opts,  # type: GetAllReplicasOptions
                     **kwargs,  # type: Dict[str, Any]
                     ) -> Iterable[GetResult]:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.GetAllReplicas), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder

        raise FeatureUnavailableException('Protostellar does not yet implement replica reads.  See ING-373.')
        # req_args = self._get_namespace_args()
        # req_args['key'] = key
        # req = v1_pb2.GetReplicaRequest(**req_args)
        # response, call = self._kv.GetReplica.with_call(req, **call_args)
        # return ProtostellarResponse(response, call, key, transcoder)

    @BlockingWrapper.decode_read_op(GetResult)
    def get_and_lock(self,
                     key,  # type: str
                     lock_time,  # type: timedelta
                     *opts,  # type: GetAndLockOptions
                     **kwargs,  # type: Dict[str, Any]
                     ) -> GetResult:
        # add to kwargs for conversion to int
        kwargs['lock_time'] = lock_time
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.GetAndLock), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder

        req_args = self._get_namespace_args()
        req_args['key'] = key
        req = v1_pb2.GetAndLockRequest(**req_args)
        response, call = self._kv.GetAndLock.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, transcoder)
    
    @BlockingWrapper.decode_read_op(GetResult)
    def get_and_touch(self,
                     key,  # type: str
                     expiry,  # type: timedelta
                     *opts,  # type: GetAndTouchOptions
                     **kwargs,  # type: Dict[str, Any]
                     ) -> GetResult:
        # add to kwargs for conversion to int
        kwargs['expiry'] = expiry
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.GetAndLock), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder

        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['expiry'] = final_args.get('expiry')
        req = v1_pb2.GetAndTouchRequest(**req_args)
        response, call = self._kv.GetAndTouch.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, transcoder)

    @BlockingWrapper.decode_read_op(GetReplicaResult)
    def get_any_replica(self,
                        key,  # type: str
                        *opts,  # type: GetAnyReplicaOptions
                        **kwargs,  # type: Dict[str, Any]
                        ) -> GetReplicaResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.GetAnyReplicas), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder

        raise FeatureUnavailableException('Protostellar does not yet implement replica reads.  See ING-373.')
        # req_args = self._get_namespace_args()
        # req_args['key'] = key
        # req = v1_pb2.GetReplicaRequest(**req_args)
        # response, call = self._kv.GetReplica.with_call(req, **call_args)
        # return ProtostellarResponse(response, call, key, transcoder)

    @BlockingWrapper.decode_mutation_op(MutationResult)
    def insert(self,
               key,  # type: str
               value,  # type: JSONType
               *opts,  # type: InsertOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Insert), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        content, content_type = transcoder.encode_value(value, implementation=self.api_implementation)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['content'] = content
        req_args['content_type'] = content_type
        if 'expiry' in final_args:
            req_args['expiry'] = final_args.get('expiry')
        req = v1_pb2.InsertRequest(**req_args)
        response, call = self._kv.Insert.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)

    @BlockingWrapper.decode_read_op(LookupInResult)
    def lookup_in(self,
                  key,  # type: str
                  spec,  # type: Iterable[Spec]
                  *opts,  # type: LookupInOptions
                  **kwargs,  # type: Dict[str, Any]
                  ) -> LookupInResult:
        if not isinstance(spec, (list, tuple)):
            raise InvalidArgumentException('Cannot perform subdoc operation, spec must be a tuple or list.')

        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.LookupIn), kwargs, *opts)
        call_args = self._get_call_args(**final_args)

        req_args = self._get_namespace_args()
        req_args['key'] = key
        req = v1_pb2.LookupInRequest(**req_args)
        for sp in spec:
            ps_spec = to_protostellar_lookup_in_spec(sp)
            req.specs.append(ps_spec)

        response, call = self._kv.LookupIn.with_call(req, **call_args)
        tc = self.default_transcoder if isinstance(self.default_transcoder, JSONTranscoder) else JSONTranscoder()
        return ProtostellarResponse(response, call, key, tc)

    @BlockingWrapper.decode_mutation_op(MutateInResult)
    def mutate_in(self,
                  key,  # type: str
                  spec,  # type: Iterable[Spec]
                  *opts,  # type: MutateInOptions
                  **kwargs,  # type: Dict[str, Any]
                  ) -> MutateInResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.MutateIn), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req = v1_pb2.MutateInRequest(**req_args)

        insert_semantics = final_args.pop('insert_doc', None)
        upsert_semantics = final_args.pop('upsert_doc', None)
        replace_semantics = final_args.pop('replace_doc', None)
        if insert_semantics is not None and (upsert_semantics is not None or replace_semantics is not None):
            raise InvalidArgumentException("Cannot set multiple store semantics.")
        if upsert_semantics is not None and (insert_semantics is not None or replace_semantics is not None):
            raise InvalidArgumentException("Cannot set multiple store semantics.")

        if insert_semantics is not None:
            req.store_semantic = v1_pb2.MutateInRequest.StoreSemantic.INSERT
        if upsert_semantics is not None:
            req.store_semantic = v1_pb2.MutateInRequest.StoreSemantic.UPSERT
        if replace_semantics is not None:
            req.store_semantic = v1_pb2.MutateInRequest.StoreSemantic.REPLACE

        tc = self.default_transcoder if isinstance(self.default_transcoder, JSONTranscoder) else JSONTranscoder()
        for sp in spec:
            ps_spec = to_protostellar_mutate_in_spec(sp, tc)
            req.specs.append(ps_spec)

        response, call = self._kv.LookupIn.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, tc)

    @BlockingWrapper.decode_mutation_op(MutationResult)
    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Remove), kwargs, *opts)
        call_args = self._get_call_args(**final_args)

        req_args = self._get_namespace_args()
        req_args['key'] = key
        if 'cas' in final_args:
            req_args['cas'] = final_args.get('cas')
        req = v1_pb2.RemoveRequest(**req_args)
        response, call = self._kv.Remove.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)

    @BlockingWrapper.decode_mutation_op(MutationResult)
    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Replace), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        content, content_type = transcoder.encode_value(value, implementation=self.api_implementation)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['content'] = content
        req_args['content_type'] = content_type
        if 'cas' in final_args:
            req_args['cas'] = final_args.get('cas')

        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', None)
        if expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                'The expiry and preserve_expiry options cannot both be set for replace operations.'
            )
            
        # @TODO:  expiry is by default set to preserve, in class it is set to reset (i.e. 0)
        if preserve_expiry is None and expiry is not None:
            req_args['expiry'] = expiry
        elif preserve_expiry is False:
            # @TODO:  how to pass in expiry = 0, since expiry is a timestamp
            # req_args['expiry'] = timedelta_as_timestamp(timedelta(seconds=0))
            pass

        req = v1_pb2.ReplaceRequest(**req_args)
        response, call = self._kv.Replace.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)

    @BlockingWrapper.decode_mutation_op(MutationResult)
    def touch(self,
               key,  # type: str
               expiry,  # type: timedelta
               *opts,  # type: TouchOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        kwargs['expiry'] = expiry
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Touch), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['expiry'] = final_args.get('expiry')
        req = v1_pb2.TouchRequest(**req_args)
        response, call = self._kv.Touch.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)

    @BlockingWrapper.decode_mutation_op(None)
    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> None:
        kwargs['cas'] = cas
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Unlock), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['cas'] = final_args.get('cas')
        req = v1_pb2.UnlockRequest(**req_args)
        response, call = self._kv.Unlock.with_call(req, **call_args)
        ProtostellarResponse(response, call, key, None)

    @BlockingWrapper.decode_mutation_op(MutationResult)
    def upsert(self,
               key,  # type: str
               value,  # type: JSONType
               *opts,  # type: UpsertOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Upsert), kwargs, *opts)
        call_args = self._get_call_args(**final_args)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        content, content_type = transcoder.encode_value(value, implementation=self.api_implementation)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['content'] = content
        req_args['content_type'] = content_type
        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', None)
        # new behavior in PS
        if expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                'The expiry and preserve_expiry options cannot both be set for upsert operations.'
            )
            
        # @TODO:  expiry is by default set to preserve, in class it is set to reset (i.e. 0)
        if preserve_expiry is None and expiry is not None:
            req_args['expiry'] = expiry
        elif preserve_expiry is False:
            # @TODO:  how to pass in expiry = 0, since expiry is a timestamp
            # req_args['expiry'] = timedelta_as_timestamp(timedelta(seconds=0))
            pass
        req = v1_pb2.UpsertRequest(**req_args)
        response, call = self._kv.Upsert.with_call(req, **call_args)
        return ProtostellarResponse(response, call, key, None)

    @staticmethod
    def default_name():
        return "_default"