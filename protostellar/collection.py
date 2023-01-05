import json

from typing import Any, Dict, Optional, TYPE_CHECKING

from couchbase.exceptions import InvalidArgumentException

from protostellar import kv_grpc_module as kv
from protostellar.proto.couchbase.kv import v1_pb2
from protostellar.options import ExistsOptions, GetOptions, parse_options, RemoveOptions, UpsertOptions
from protostellar.result import ExistsResult, GetResult, MutationResult
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

    def get(self, 
        key, # type: str
        options=None, # type: Optional[GetOptions]
        **kwargs, # type: Dict[str, Any]
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

    def exists(
        self,
        key,  # type: str
        options=None, # type: Optional[ExistsOptions]
        **kwargs, # type: Dict[str, Any]
    ) -> ExistsResult:
        final_opts = parse_options(options, **kwargs)
        req_args = self._get_namespace_args()
        req_args['key'] = key
        # TODO:  add final_opts
        req = v1_pb2.ExistsRequest(**req_args)
        res = self._kv.Exists(req)
        return ExistsResult(key, res)

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

        req = v1_pb2.UpsertRequest(**req_args)
        res = self._kv.Upsert(req)
        return MutationResult(key, res)

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

    @staticmethod
    def default_name():
        return "_default"