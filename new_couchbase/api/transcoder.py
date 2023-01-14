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
from typing import Any, Dict, Tuple


class TranscoderInterface(ABC):
    """Interface a Custom Transcoder must implement
    """

    @abstractmethod
    def encode_value(self,
                     value,   # type: Any
                     **kwargs, # type: Dict[str, Any]
                     ) -> Tuple[bytes, int]:
        raise NotImplementedError

    @abstractmethod
    def decode_value(self,
                     value,  # type: bytes
                     content_type,  # type: int
                     **kwargs, # type: Dict[str, Any]
                     ) -> Any:
        raise NotImplementedError

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'encode_value') and
                callable(subclass.encode_value) and
                hasattr(subclass, 'decode_value') and
                callable(subclass.decode_value))