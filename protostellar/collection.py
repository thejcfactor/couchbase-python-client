from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional)

from couchbase.exceptions import InvalidArgumentException
from protostellar import kv_grpc_module as kv
from protostellar.logic import BlockingWrapper
from protostellar.options import (ExistsOptions,
                                  GetAndLockOptions,
                                  GetAndTouchOptions,
                                  GetOptions,
                                  InsertOptions,
                                  LookupInOptions,
                                  MutateInOptions,
                                  RemoveOptions,
                                  ReplaceOptions,
                                  TouchOptions,
                                  UnlockOptions,
                                  UpsertOptions,
                                  parse_options)
from protostellar.proto.couchbase.kv import v1_pb2
from protostellar.result import (ExistsResult,
                                 GetResult,
                                 LookupInResult,
                                 MutateInResult,
                                 MutationResult)
from protostellar.subdocument import Spec, to_protostellar_lookup_in_spec, to_protostellar_mutate_in_spec
from protostellar.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase._utils import JSONType


class Collection:
    def __init__(self, scope, name):
        if not scope:
            raise InvalidArgumentException(message="Collection must be given a scope")
        # if not scope.connection:
        #     raise RuntimeError("No connection provided")
        self._scope = scope
        self._collection_name = name
        self._channel = scope.channel
        self._kv = kv.KvStub(self._channel)

    @property
    def name(self) -> str:
        return self._collection_name

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._scope.default_transcoder

    def _get_namespace_args(self) -> Dict[str, str]:
        return {
            'bucket_name': self._scope.bucket_name,
            'scope_name': self._scope.name,
            'collection_name': self.name,
        }

    @BlockingWrapper.handle_exception
    def exists(
        self,
        key,  # type: str
        options=None,  # type: Optional[ExistsOptions]
        **kwargs,  # type: Dict[str, Any]
    ) -> ExistsResult:
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        # TODO:  add final_opts
        req = v1_pb2.ExistsRequest(**req_args)
        res = self._kv.Exists(req)
        return ExistsResult(key, res)

    @BlockingWrapper.handle_exception
    def get(self,
            key,  # type: str
            options=None,  # type: Optional[GetOptions]
            **kwargs,  # type: Dict[str, Any]
            ) -> GetResult:
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        # TODO:  add final_opts
        req = v1_pb2.GetRequest(**req_args)
        res = self._kv.Get(req)
        transcoder = final_opts.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        return GetResult(key, res, transcoder)

    @BlockingWrapper.handle_exception
    def get_and_lock(self,
                     key,  # type: str
                     lock_time,  # type: timedelta
                     options=None,  # type: Optional[GetAndLockOptions]
                     **kwargs,  # type: Dict[str, Any]
                     ) -> GetResult:
        kwargs['lock_time'] = lock_time
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        # TODO:  add final_opts
        req = v1_pb2.GetAndLockRequest(**req_args)
        res = self._kv.GetAndLock(req)
        transcoder = final_opts.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        return GetResult(key, res, transcoder)

    @BlockingWrapper.handle_exception
    def get_and_touch(self,
                      key,  # type: str
                      expiry,  # type: timedelta
                      options=None,  # type: Optional[GetAndTouchOptions]
                      **kwargs,  # type: Dict[str, Any]
                      ) -> GetResult:
        kwargs['expiry'] = expiry
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        # TODO:  add final_opts
        req = v1_pb2.GetAndTouchRequest(**req_args)
        res = self._kv.GetAndTouch(req)
        transcoder = final_opts.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        return GetResult(key, res, transcoder)

    @BlockingWrapper.handle_exception
    def insert(
        self,
        key,  # type: str
        value,  # type: JSONType
        options=None,  # type: Optional[InsertOptions]
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        final_opts = parse_options(options, **kwargs)
        transcoder = final_opts.pop('transcoder', self.default_transcoder)
        content, content_type = transcoder.encode_value(value)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['content'] = content
        req_args['content_type'] = content_type

        durability = final_opts.pop('durability', None)
        if durability is not None:
            if isinstance(durability, int):
                req_args['durability_level'] = durability
            elif isinstance(durability, v1_pb2.LegacyDurabilitySpec):
                req_args['legacy_durability_spec'] = durability

        if final_opts.get('expiry', None):
            req_args['expiry'] = final_opts.get('expiry')

        req = v1_pb2.InsertRequest(**req_args)
        res = self._kv.Insert(req)
        return MutationResult(key, res)

    @BlockingWrapper.handle_exception
    def remove(self,
               key,  # type: str
               options=None,  # type: Optional[RemoveOptions]
               **kwargs,  # type: Dict[str, Any]
               ) -> Optional[MutationResult]:
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key

        if final_opts.get('cas', None):
            req_args['cas'] = final_opts.get('cas')

        durability = final_opts.pop('durability', None)
        if durability is not None:
            if isinstance(durability, int):
                req_args['durability_level'] = durability
            elif isinstance(durability, v1_pb2.LegacyDurabilitySpec):
                req_args['legacy_durability_spec'] = durability

        req = v1_pb2.RemoveRequest(**req_args)
        res = self._kv.Remove(req)
        return MutationResult(key, res)

    @BlockingWrapper.handle_exception
    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                options=None,  # type: Optional[ReplaceOptions]
                **kwargs,  # type: Dict[str, Any]
                ) -> Optional[MutationResult]:
        final_opts = parse_options(options, **kwargs)
        transcoder = final_opts.pop('transcoder', self.default_transcoder)
        content, content_type = transcoder.encode_value(value)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['content'] = content
        req_args['content_type'] = content_type

        if final_opts.get('cas', None):
            req_args['cas'] = final_opts.get('cas')

        durability = final_opts.pop('durability', None)
        if durability is not None:
            if isinstance(durability, int):
                req_args['durability_level'] = durability
            elif isinstance(durability, v1_pb2.LegacyDurabilitySpec):
                req_args['legacy_durability_spec'] = durability

        if final_opts.get('expiry', None):
            req_args['expiry'] = final_opts.get('expiry')

        req = v1_pb2.ReplaceRequest(**req_args)
        res = self._kv.Replace(req)
        return MutationResult(key, res)

    @BlockingWrapper.handle_exception
    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              options=None,  # type: Optional[TouchOptions]
              **kwargs,  # type: Dict[str, Any]
              ) -> MutationResult:
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        if final_opts.get('expiry', None):
            req_args['expiry'] = final_opts.get('expiry')
        req = v1_pb2.TouchRequest(**req_args)
        res = self._kv.Touch(req)
        return MutationResult(key, res)

    @BlockingWrapper.handle_exception
    def unlock(self,
               key,  # type: str
               cas,  # type: int
               options=None,  # type: Optional[UnlockOptions]
               **kwargs,  # type: Dict[str, Any]
               ) -> None:
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['cas'] = cas
        req = v1_pb2.UnlockRequest(**req_args)
        self._kv.Unlock(req)

    @BlockingWrapper.handle_exception
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        options=None,  # type: Optional[UpsertOptions]
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        final_opts = parse_options(options, **kwargs)
        transcoder = final_opts.pop('transcoder', self.default_transcoder)
        content, content_type = transcoder.encode_value(value)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req_args['content'] = content
        req_args['content_type'] = content_type

        durability = final_opts.pop('durability', None)
        if durability is not None:
            if isinstance(durability, int):
                req_args['durability_level'] = durability
            elif isinstance(durability, v1_pb2.LegacyDurabilitySpec):
                req_args['legacy_durability_spec'] = durability

        if final_opts.get('expiry', None):
            req_args['expiry'] = final_opts.get('expiry')

        req = v1_pb2.UpsertRequest(**req_args)
        res = self._kv.Upsert(req)
        return MutationResult(key, res)

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        options=None,  # type: Optional[LookupInOptions]
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInResult:
        if not isinstance(spec, (list, tuple)):
            raise InvalidArgumentException('Cannot perform subdoc operation, spec must be a tuple or list.')

        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req = v1_pb2.LookupInRequest(**req_args)
        for sp in spec:
            ps_spec = to_protostellar_lookup_in_spec(sp)
            req.specs.append(ps_spec)

        res = self._kv.LookupIn(req)
        return LookupInResult(res)

    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        options=None,  # type: Optional[MutateInOptions]
        **kwargs,  # type: Dict[str, Any]
    ) -> MutateInResult:
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        req = v1_pb2.MutateInRequest(**req_args)

        insert_semantics = final_opts.pop('insert_doc', None)
        upsert_semantics = final_opts.pop('upsert_doc', None)
        replace_semantics = final_opts.pop('replace_doc', None)
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

        for sp in spec:
            ps_spec = to_protostellar_mutate_in_spec(sp)
            req.specs.append(ps_spec)

        res = self._kv.MutateIn(req)
        return MutateInResult(res)

    @staticmethod
    def default_name():
        return "_default"
