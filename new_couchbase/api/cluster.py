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


from new_couchbase.api.bucket import BucketInterface
from new_couchbase.api.result import DiagnosticsResultInterface, PingResultInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.api import ApiImplementation


if TYPE_CHECKING:
    from new_couchbase.options import DiagnosticsOptions, PingOptions


class ClusterCoreInterface(ABC):
    """Cluster Core Interface
    """

    @abstractmethod
    def diagnostics(self,
                    *opts,  # type: Optional[DiagnosticsOptions]
                    **kwargs  # type: Dict[str, Any]
                    ) -> DiagnosticsResultInterface:
        raise NotImplementedError

    @abstractmethod
    def ping(self,
             *opts,  # type: Optional[PingOptions]
             **kwargs  # type: Dict[str, Any]
             ) -> PingResultInterface:
        raise NotImplementedError 

class ClusterInterface(ABC):
    """Cluster Interface
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
    def default_transcoder(self) -> TranscoderInterface:
        raise NotImplementedError

    @property
    @abstractmethod
    def default_serializer(self) -> SerializerInterface:
        raise NotImplementedError

    @property
    @abstractmethod
    def server_version(self) -> Optional[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def server_version_short(self) -> Optional[float]:
        raise NotImplementedError

    @property
    @abstractmethod
    def server_version_full(self) -> Optional[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_developer_preview(self) -> Optional[bool]:
        raise NotImplementedError

    @abstractmethod
    def bucket(self,
               bucket_name # type: str
               ) -> BucketInterface:
        raise NotImplementedError

    @abstractmethod
    def diagnostics(self,
                    *opts,  # type: Optional[DiagnosticsOptions]
                    **kwargs  # type: Dict[str, Any]
                    ) -> DiagnosticsResultInterface:
        raise NotImplementedError

    @abstractmethod
    def ping(self,
             *opts,  # type: Optional[PingOptions]
             **kwargs  # type: Dict[str, Any]
             ) -> PingResultInterface:
        raise NotImplementedError  
