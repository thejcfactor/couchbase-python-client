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

import json
from datetime import datetime
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional)

from new_couchbase.api.result import GetResultInterface, MutationResultInterface
from new_couchbase.common.mutation_state import MutationToken

if TYPE_CHECKING:
    from couchbase.pycbc_core import result

"""

Python SDK Key-Value Results

"""


class ContentProxy:
    """
    Used to provide access to Result content via Result.content_as[type]
    """

    def __init__(self, content):
        self._content = content

    def __getitem__(self,
                    type_       # type: Any
                    ) -> Any:
        """

        :param type_: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return type_(self._content)



class GetResult(GetResultInterface):
    def __init__(self, orig # type: result
        ):
        self._orig = orig

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get("cas", None)

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get(key)
                value = res.content_as[dict]

        """
        return ContentProxy(self.value)

    @property
    def expiry_time(self) -> Optional[datetime]:
        """
            Optional[datetime]: The expiry of the document, if it was requested.
        """
        time_ms = self._orig.raw_result.get("expiry", None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def expiryTime(self) -> Optional[datetime]:
        """
        ** DEPRECATED ** use expiry_time

        Optional[datetime]: The expiry of the document, if it was requested.
        """
        # make this a datetime!
        time_ms = self._orig.raw_result.get("expiry", None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get("flags", None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get("key", None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._orig.raw_result.get("value", None)

    def __repr__(self):
        return "GetResult:{}".format(self._orig)

class MutationResult(MutationResultInterface):
    def __init__(self, 
                orig # type: result
                ):
        self._orig = orig
        self._raw_mutation_token = self._orig.raw_result.get('mutation_token', None)
        self._mutation_token = None

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get("cas", None)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get("flags", None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get("key", None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas is not None and self.cas != 0

    def mutation_token(self) -> Optional[MutationToken]:
        """Get the operation's mutation token, if it exists.

        Returns:
            Optional[:class:`.MutationToken`]: The operation's mutation token.
        """
        if self._raw_mutation_token is not None and self._mutation_token is None:
            self._mutation_token = MutationToken(self._raw_mutation_token.get())
        return self._mutation_token

    def __repr__(self):
        return "MutationResult:{}".format(self._orig)