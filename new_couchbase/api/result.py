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
from datetime import datetime
from typing import Any, Dict, Optional

from new_couchbase.common.diagnostics import ClusterState
from new_couchbase.common.mutation_state import MutationToken

class DiagnosticsResultInterface(ABC):

    @property
    @abstractmethod
    def endpoints(self) -> Dict[str, Any]:
        raise NotImplementedError

    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def sdk(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def state(self) -> ClusterState:
        raise NotImplementedError

    @property
    @abstractmethod
    def success(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def version(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def as_json(self) -> str:
        raise NotImplementedError


class PingResultInterface(ABC):

    @property
    @abstractmethod
    def endpoints(self) -> Dict[str, Any]:
        raise NotImplementedError

    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def sdk(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def success(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def version(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def as_json(self) -> str:
        raise NotImplementedError

class GetResultInterface(ABC):

    @property
    @abstractmethod
    def cas(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def content_as(self) -> Any:
        raise NotImplementedError

    @property
    @abstractmethod
    def expiry_time(self) -> Optional[datetime]:
        raise NotImplementedError

    @property
    @abstractmethod
    def flags(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def key(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def success(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def value(self) -> Any:
        raise NotImplementedError

class MutationResultInterface(ABC):

    @property
    @abstractmethod
    def cas(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def flags(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def key(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def mutation_token(self) -> Optional[MutationToken]:
        raise NotImplementedError

    @property
    @abstractmethod
    def success(self) -> bool:
        raise NotImplementedError
