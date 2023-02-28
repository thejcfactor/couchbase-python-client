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

import json
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional,
                    Union)

from couchbase.pycbc_core import (binary_operation,
                                  kv_operation,
                                  operations,
                                  subdoc_operation)

from new_couchbase.exceptions import InvalidArgumentException
from new_couchbase.transcoder import Transcoder
from new_couchbase.classic.result import (CounterResult,
                                          ExistsResult,
                                          GetReplicaResult,
                                          GetResult,
                                          LookupInResult,
                                          MutateInResult,
                                          MutationResult)
from new_couchbase.common._utils import timedelta_as_microseconds
from new_couchbase.subdocument import (Spec,
                                   StoreSemantics,
                                   SubDocOp)

if TYPE_CHECKING:
    from new_couchbase.classic.scope import Scope
    from new_couchbase.common._utils import JSONType
    from new_couchbase.subdocument import Spec

class CollectionCore:
    """
    **INTERNAL**
    Not a part of the public API.
    """
    def __init__(self, 
                scope, # type: Scope
                name # type: str
                ):
        if not scope:
            raise InvalidArgumentException(message="Collection must be given a scope")
        # if not scope.connection:
        #     raise RuntimeError("No connection provided")
        self._scope = scope
        self._collection_name = name
        self._connection = scope.connection

    @property
    def default_transcoder(self) -> Transcoder:
        """
        **INTERNAL**
        """
        return self._scope.default_transcoder

    @property
    def name(self) -> str:
        """
        **INTERNAL**
        """
        return self._collection_name

    def _get_connection_args(self) -> Dict[str, Any]:
        """
        **INTERNAL**
        """
        return {
            "conn": self._connection,
            "bucket": self._scope.bucket_name,
            "scope": self._scope.name,
            "collection_name": self.name
        }

    def _parse_durability_timeout(self,
                              **kwargs  # type: Dict[str, Any]
                              ) -> Dict[str, Any]:
        """**INTERNAL**
        Parses the mutaiton operation options.  If synchronous durability has been set and no timeout provided, the
        default timeout will be set to the default KV durable timeout (10 seconds).
        """
        if 'durability' in kwargs and isinstance(kwargs['durability'], int) and 'timeout' not in kwargs:
            kwargs['timeout'] = timedelta_as_microseconds(timedelta(seconds=10))

        return kwargs

    # this is utilized by the async collection
    def set_connection(self):
        """
        **INTERNAL**
        """
        self._connection = self._scope.connection

    def append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        **kwargs,  # type: Dict[str, Any]
    ) -> Optional[MutationResult]:
        final_args = self._parse_durability_timeout(**kwargs)
        if isinstance(value, str):
            value = value.encode('utf-8')
        elif isinstance(value, bytearray):
            value = bytes(value)

        if not isinstance(value, bytes):
            raise ValueError("The value provided must of type str, bytes or bytearray.")

        op_type = operations.APPEND.value
        return binary_operation(**self._get_connection_args(),
                                key=key,
                                op_type=op_type,
                                value=value,
                                op_args=final_args)

    def decrement(self,
                  key,  # type: str
                  **kwargs,  # type: Dict[str, Any]
                  ) -> Optional[CounterResult]:
        final_args = self._parse_durability_timeout(**kwargs)

        op_type = operations.DECREMENT.value
        final_args['initial'] = int(final_args['initial'])
        final_args['delta'] = int(final_args['delta'])
        return binary_operation(**self._get_connection_args(),
                                key=key,
                                op_type=op_type,
                                op_args=final_args)

    def increment(self,
                  key,  # type: str
                  **kwargs,  # type: Dict[str, Any]
    ) -> Optional[CounterResult]:
        final_args = self._parse_durability_timeout(**kwargs)

        op_type = operations.INCREMENT.value
        final_args['initial'] = int(final_args['initial'])
        final_args['delta'] = int(final_args['delta'])
        return binary_operation(**self._get_connection_args(),
                                key=key,
                                op_type=op_type,
                                op_args=final_args)

    def prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        **kwargs,  # type: Dict[str, Any]
    ) -> Optional[MutationResult]:
        final_args = self._parse_durability_timeout(**kwargs)
        if isinstance(value, str):
            value = value.encode('utf-8')
        elif isinstance(value, bytearray):
            value = bytes(value)

        if not isinstance(value, bytes):
            raise ValueError(
                "The value provided must of type str, bytes or bytearray.")

        op_type = operations.PREPEND.value
        return binary_operation(**self._get_connection_args(),
                                key=key,
                                op_type=op_type,
                                value=value,
                                op_args=final_args)

    def exists(self,
               key,  # type: str
               **kwargs,  # type: Dict[str, Any]
               ) -> ExistsResult:
        """
        **INTERNAL**
        """
        op_type = operations.EXISTS.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=kwargs
        )

    def get(self,
            key,  # type: str
            **kwargs,  # type: Dict[str, Any]
            ) -> GetResult:
        """
        **INTERNAL**
        """
        op_type = operations.GET.value
        return kv_operation(**self._get_connection_args(),
                            key=key,
                            op_type=op_type,
                            op_args=kwargs)

    def get_all_replicas(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> Iterable[GetReplicaResult]:
        """**INTERNAL**

        Key-Value *get_all_replicas* operation.  Should only be called by classes that inherit from the base
            class :class:`~couchbase.logic.CollectionLogic`.

        Args:
            key (str): document key
            kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                overrride provided :class:`~.options.GetAllReplicasOptions`

        Raises:
            :class:`~.exceptions.DocumentNotFoundException`: If the provided document key does not exist.
        """
        op_type = operations.GET_ALL_REPLICAS.value
        return kv_operation(**self._get_connection_args(),
                            key=key,
                            op_type=op_type,
                            op_args=kwargs)

    def get_and_lock(self,
                     key,  # type: str
                     **kwargs,  # type: Dict[str, Any]
                     ) -> GetResult:
        op_type = operations.GET_AND_LOCK.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=kwargs
        )

    def get_and_touch(self,
                      key,  # type: str
                      **kwargs,  # type: Dict[str, Any]
                      ) -> GetResult:
        op_type = operations.GET_AND_TOUCH.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=kwargs
        )
    
    def get_any_replica(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> GetReplicaResult:
        """**INTERNAL**

        Key-Value *get_any_replica* operation.  Should only be called by classes that inherit from the base
            class :class:`~couchbase.logic.CollectionLogic`.

        Args:
            key (str): document key
            kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                overrride provided :class:`~.options.GetAnyReplicaOptions`

        Raises:
            :class:`~.exceptions.DocumentNotFoundException`: If the provided document key does not exist.
        """
        op_type = operations.GET_ANY_REPLICA.value
        return kv_operation(**self._get_connection_args(),
                            key=key,
                            op_type=op_type,
                            op_args=kwargs)

    def insert(self,
               key,  # type: str
               value,  # type: JSONType
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        """
        **INTERNAL**
        """
        final_args = self._parse_durability_timeout(**kwargs)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        transcoded_value = transcoder.encode_value(value)
        op_type = operations.INSERT.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            value=transcoded_value,
            op_type=op_type,
            op_args=final_args
        )

    def lookup_in(self,
                  key,  # type: str
                  spec,  # type: Iterable[Spec]
                  **kwargs,  # type: Dict[str, Any]
                  ) -> LookupInResult:
        """
        **INTERNAL**
        """
        op_type = operations.LOOKUP_IN.value
        return subdoc_operation(
            **self._get_connection_args(),
            key=key,
            spec=spec,
            op_type=op_type,
            op_args=kwargs
        )

    def mutate_in(self,  # noqa: C901
                  key,  # type: str
                  spec,  # type: Iterable[Spec]
                  **kwargs,  # type: Dict[str, Any]
                  ) -> MutateInResult:   # noqa: C901
        """
        **INTERNAL**
        """
        # no tc for sub-doc, use default JSON
        final_args = self._parse_durability_timeout(**kwargs)
        transcoder = final_args.pop('transcoder', self.default_transcoder)

        expiry = final_args.get('expiry', None)
        preserve_expiry = final_args.get('preserve_expiry', False)

        spec_ops = [s[0] for s in spec]
        if SubDocOp.DICT_ADD in spec_ops and preserve_expiry is True:
            raise InvalidArgumentException(
                'The preserve_expiry option cannot be set for mutate_in with insert operations.')

        if SubDocOp.REPLACE in spec_ops and expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                'The expiry and preserve_expiry options cannot both be set for mutate_in with replace operations.')

        """
            @TODO(jc): document that the kwarg will override option:
            await cb.mutate_in(key,
                (SD.upsert('new_path', 'im new'),),
                MutateInOptions(store_semantics=SD.StoreSemantics.INSERT),
                upsert_doc=True)

                will set store_semantics to be UPSERT
        """

        insert_semantics = final_args.pop('insert_doc', None)
        upsert_semantics = final_args.pop('upsert_doc', None)
        replace_semantics = final_args.pop('replace_doc', None)
        if insert_semantics is not None and (upsert_semantics is not None or replace_semantics is not None):
            raise InvalidArgumentException("Cannot set multiple store semantics.")
        if upsert_semantics is not None and (insert_semantics is not None or replace_semantics is not None):
            raise InvalidArgumentException("Cannot set multiple store semantics.")

        if insert_semantics is not None:
            final_args["store_semantics"] = StoreSemantics.INSERT
        if upsert_semantics is not None:
            final_args["store_semantics"] = StoreSemantics.UPSERT
        if replace_semantics is not None:
            final_args["store_semantics"] = StoreSemantics.REPLACE

        final_spec = []
        allowed_multi_ops = [SubDocOp.ARRAY_PUSH_FIRST,
                             SubDocOp.ARRAY_PUSH_LAST,
                             SubDocOp.ARRAY_ADD_UNIQUE,
                             SubDocOp.ARRAY_INSERT]

        for s in spec:
            if len(s) == 6:
                tmp = list(s[:5])
                if s[0] in allowed_multi_ops:
                    new_value = json.dumps(s[5], ensure_ascii=False)
                    # this is an array, need to remove brackets
                    tmp.append(new_value[1:len(new_value)-1].encode('utf-8'))
                else:
                    # no need to propagate the flags
                    tmp.append(transcoder.encode_value(s[5])[0])
                final_spec.append(tuple(tmp))
            else:
                final_spec.append(s)

        op_type = operations.MUTATE_IN.value
        return subdoc_operation(
            **self._get_connection_args(),
            key=key,
            spec=final_spec,
            op_type=op_type,
            op_args=final_args
        )

    def remove(self,
               key,  # type: str
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        """
        **INTERNAL**
        """
        final_args = self._parse_durability_timeout(**kwargs)
        op_type = operations.REMOVE.value
        return kv_operation(
            **self._get_connection_args(), key=key, op_type=op_type, op_args=final_args
        )

    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                **kwargs,  # type: Dict[str, Any]
                ) -> MutationResult:
        """
        **INTERNAL**
        """
        final_args = self._parse_durability_timeout(**kwargs)
        expiry = final_args.get("expiry", None)
        preserve_expiry = final_args.get("preserve_expiry", False)
        if expiry and preserve_expiry is True:
            raise InvalidArgumentException(
                "The expiry and preserve_expiry options cannot both be set for replace operations."
            )

        transcoder = final_args.pop('transcoder', self.default_transcoder)
        transcoded_value = transcoder.encode_value(value)

        op_type = operations.REPLACE.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            value=transcoded_value,
            op_type=op_type,
            op_args=final_args
        )
    
    def touch(self,
              key,  # type: str
              **kwargs,  # type: Dict[str, Any]
              ) -> MutationResult:
        op_type = operations.TOUCH.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            op_type=op_type,
            op_args=kwargs
        )

    def unlock(self,
               key, # type: str
               **kwargs,  # type: Dict[str, Any]
               ) -> None:
        op_type = operations.UNLOCK.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            op_type=op_type,
            op_args=kwargs
        )

    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        """
        **INTERNAL**
        """
        final_args = self._parse_durability_timeout(**kwargs)
        transcoder = final_args.pop('transcoder', self.default_transcoder)
        transcoded_value = transcoder.encode_value(value)

        op_type = operations.UPSERT.value
        return kv_operation(
            **self._get_connection_args(),
            key=key,
            value=transcoded_value,
            op_type=op_type,
            op_args=final_args
        )