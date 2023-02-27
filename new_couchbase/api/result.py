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

from new_couchbase.diagnostics import ClusterState
from new_couchbase.mutation_state import MutationToken

"""

Python SDK Diagnostic Operation Results

"""

class ClusterInfoResultInterface(ABC):

    @property
    @abstractmethod
    def is_community(self) -> Optional[bool]:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_enterprise(self) -> Optional[bool]:
        raise NotImplementedError

    @property
    @abstractmethod
    def nodes(self) -> Any:
        raise NotImplementedError

    @property
    @abstractmethod
    def server_version(self) -> Optional[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def server_version_build(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def server_version_full(self) -> Optional[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def server_version_short(self) -> Optional[float]:
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


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

    @abstractmethod
    def __repr__(self) -> str:
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

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError

"""

Python SDK Key-Value Operation Results

"""

# Binary Operations

# Multi Operations

# Standard Operations

class ExistsResultInterface(ABC):

    @property
    @abstractmethod
    def cas(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def exists(self) -> bool:
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

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError
        

class GetReplicaResultInterface(ABC):

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
    def flags(self) -> Optional[int]:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_replica(self) -> bool:
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

    @abstractmethod
    def __repr__(self) -> str:
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

    @abstractmethod
    def __repr__(self) -> str:
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

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


# Sub-document Operations

class LookupInResultInterface(ABC):

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

    @abstractmethod
    def exists(self,
              index  # type: int
            ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError

class MutateInResultInterface(ABC):

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

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


"""

Python SDK Streaming Results

"""

class QueryResultInterface(ABC):

    @abstractmethod
    def execute(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def metadata(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def rows(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError