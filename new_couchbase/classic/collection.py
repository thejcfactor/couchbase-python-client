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

from typing import TYPE_CHECKING, Any, Dict, Iterable


from new_couchbase.classic.core import BlockingWrapper
from new_couchbase.classic.exceptions import exception as BaseCouchbaseException
from new_couchbase.classic.exceptions import ErrorMapper

from new_couchbase.api import ApiImplementation

from new_couchbase.classic.core.collection import CollectionCore
from new_couchbase.classic.result import (ExistsResult,
                                          GetResult,
                                          LookupInResult,
                                          MutateInResult,
                                          MutationResult)

from new_couchbase.common.options import OptionTypes, parse_options
from new_couchbase.classic.options import ValidKeyValueOptions


if TYPE_CHECKING:
    from new_couchbase.classic.scope import Scope
    from new_couchbase.common._utils import JSONType
    from new_couchbase.options import (ExistsOptions,
                                        GetOptions,
                                        InsertOptions,
                                        LookupInOptions,
                                        MutateInOptions,
                                        RemoveOptions,
                                        ReplaceOptions,
                                        UpsertOptions)
    from new_couchbase.subdocument import Spec

class Collection(CollectionCore):
    def __init__(self,
                 scope, #type: Scope
                 collection_name # type: str
                 ) -> None:
        super().__init__(scope, collection_name)

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.CLASSIC

    @BlockingWrapper.block_and_decode(GetResult)
    def _get_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> GetResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.get` instead.
        """
        return super().get(key, **kwargs)

    @BlockingWrapper.block_and_decode(LookupInResult)
    def _lookup_in_internal(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.lookup_in` instead.

        """
        return super().lookup_in(key, spec, **kwargs)

    @BlockingWrapper.block(ExistsResult)
    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> ExistsResult:
        return super().exists(key, *opts, **kwargs)

    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Dict[str, Any]
            ) -> GetResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Get), kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder

        return self._get_internal(key, **final_args)

    @BlockingWrapper.block(MutationResult)
    def insert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Insert), kwargs, *opts)
        return super().insert(key, value, **final_args)

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.LookupIn), kwargs, *opts)
        transcoder = final_args.get('transcoder', None)
        if not transcoder:
            transcoder = self.default_transcoder
        final_args['transcoder'] = transcoder
        return self._lookup_in_internal(key, spec, **final_args)

    @BlockingWrapper.block(MutateInResult)
    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutateInResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.MutateIn), kwargs, *opts)
        return super().mutate_in(key, spec, **final_args)

    @BlockingWrapper.block(MutationResult)
    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Remove), kwargs, *opts)
        return super().remove(key, **final_args)

    @BlockingWrapper.block(MutationResult)
    def replace(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: ReplaceOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Replace), kwargs, *opts)
        return super().replace(key, value, **final_args)

    @BlockingWrapper.block(MutationResult)
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Upsert), kwargs, *opts)
        return super().upsert(key, value, **final_args)

    @staticmethod
    def default_name():
        return "_default"