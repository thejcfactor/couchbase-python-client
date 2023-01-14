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

from typing import TYPE_CHECKING, Any, Dict


from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.impl.server.core import BlockingWrapper
from new_couchbase.impl.server.exceptions import exception as BaseCouchbaseException
from new_couchbase.impl.server.exceptions import ErrorMapper

from new_couchbase.api import ApiImplementation

from new_couchbase.collection import Collection
from new_couchbase.impl.server.result import GetResult, MutationResult

from new_couchbase.common.options import OptionTypes, parse_options
from new_couchbase.impl.server.options import ValidKeyValueOptions


if TYPE_CHECKING:
    from new_couchbase.impl.server.core.collection import CollectionCore
    from new_couchbase.options import GetOptions, UpsertOptions
    from new_couchbase.common._utils import JSONType

class Collection(CollectionInterface):
    def __init__(self,
                 core: CollectionCore
                 ) -> Collection:
        self._core = core

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.SERVER

    @property
    def default_transcoder(self) -> TranscoderInterface:
        return self._core.default_transcoder

    @property
    def name(self) -> str:
        return self._core.name

    @BlockingWrapper.block_and_decode(GetResult)
    def _get_internal(
        self,
        key,  # type: str
        **kwargs,  # type: Dict[str, Any]
    ) -> GetResult:
        """ **Internal Operation**

        Internal use only.  Use :meth:`Collection.get` instead.
        """
        return self._core.get(key, **kwargs)

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
    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        final_args = parse_options(ValidKeyValueOptions.get_valid_options(OptionTypes.Upsert), kwargs, *opts)
        return self._core.upsert(key, value, **final_args)

    @staticmethod
    def default_name():
        return "_default"