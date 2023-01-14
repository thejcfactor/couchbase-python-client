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

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, TYPE_CHECKING


from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.scope import ScopeInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.api import ApiImplementation


if TYPE_CHECKING:
    from new_couchbase.common.options import DiagnosticsOptionsBase, PingOptionsBase


class BucketInterface(ABC):
    """Bucket Interface
    """

    @property
    @abstractmethod
    def api_implementation(self) -> ApiImplementation:
        raise NotImplementedError

    @property
    @abstractmethod
    def connected(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def default_serializer(self) -> SerializerInterface:
        raise NotImplementedError

    @property
    @abstractmethod
    def default_transcoder(self) -> TranscoderInterface:
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def collection(self, 
                collection_name # type: str
                ) -> CollectionInterface:
        raise NotImplementedError

    @abstractmethod
    def default_collection(self) -> CollectionInterface:
        raise NotImplementedError

    @abstractmethod
    def default_scope(self) -> ScopeInterface:
        raise NotImplementedError

    @abstractmethod
    def scope(self, 
                scope_name # type: str
                ) -> ScopeInterface:
        raise NotImplementedError
