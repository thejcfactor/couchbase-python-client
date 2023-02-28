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
from collections import namedtuple
from datetime import datetime
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional)

from new_couchbase.protostellar.proto.couchbase.kv import v1_pb2
from grpc import StatusCode


from new_couchbase.api.result import (CounterResultInterface,
                                      ExistsResultInterface,
                                      GetReplicaResultInterface,
                                      GetResultInterface,
                                      LookupInResultInterface,
                                      MutateInResultInterface,
                                      MutationResultInterface,
                                      QueryResultInterface)
from new_couchbase.exceptions import (DocumentNotFoundException,
                                        InvalidIndexException,
                                        PathExistsException,
                                        PathMismatchException,
                                        PathNotFoundException,
                                        SubdocCantInsertValueException)
from new_couchbase.mutation_state import MutationToken
from new_couchbase.subdocument import SubDocStatus
from new_couchbase.protostellar._utils import timestamp_as_datetime

if TYPE_CHECKING:
    from new_couchbase.protostellar.n1ql import N1QLRequest


"""

Python SDK Key-Value Results

"""

ProtostellarResponse = namedtuple('ProtostellarResponse', ['response', 'call', 'key', 'transcoder'])

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

class CounterResult(CounterResultInterface):
    def __init__(self,
                raw_result # type: Dict[str, Any]
        ):
        self._raw_result = raw_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._raw_result.get('cas', None)

    @property
    def content(self) -> Optional[int]:
        """
            Optional[int]: The value of the document after the operation completed.
        """
        return self._raw_result.get('content', None)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._raw_result.get('key', None)

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
        return self._raw_result.get('value', None)

    def __repr__(self):
        output = {k: v for k, v in self._raw_result.items() if v is not None}
        return 'ExistsResult:{}'.format(output)

class ExistsResult(ExistsResultInterface):
    def __init__(self,
                raw_result # type: Dict[str, Any]
        ):
        self._raw_result = raw_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._raw_result.get('cas', None)

    @property
    def exists(self) -> bool:
        """
            bool: True if the document exists, false otherwise.
        """
        return self._raw_result.get('result', False)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._raw_result.get('key', None)

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
        return self._raw_result.get('value', None)

    def __repr__(self):
        output = {k: v for k, v in self._raw_result.items() if v is not None}
        return 'ExistsResult:{}'.format(output)

class GetReplicaResult(GetReplicaResultInterface):
    def __init__(self,
                raw_result # type: Dict[str, Any]
        ):
        self._raw_result = raw_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._raw_result.get('cas', None)

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
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._raw_result.get('flags', None)

    @property
    def is_replica(self) -> bool:
        """
            bool: True if the result is a replica, False otherwise.
        """
        return self._raw_result.get('is_replica', False)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._raw_result.get('key', None)

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
        return self._raw_result.get('value', None)

    def __repr__(self):
        output = {k: v for k, v in self._raw_result.items() if v is not None}
        return 'GetReplicaResult:{}'.format(output)


class GetResult(GetResultInterface):
    def __init__(self,
                raw_result # type: Dict[str, Any]
        ):
        self._raw_result = raw_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._raw_result.get('cas', None)

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
        return self._raw_result.get('expiry', None)


    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._raw_result.get('key', None)

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
        return self._raw_result.get('value', None)

    def __repr__(self):
        output = {k: v for k, v in self._raw_result.items() if v is not None}
        return 'GetResult:{}'.format(output)

class MutationResult(MutationResultInterface):
    def __init__(self,
                raw_result # type: Dict[str, Any]
        ):
        self._raw_result = raw_result
        self._raw_mutation_token = self._raw_result.get('mutation_token', None)
        self._mutation_token = None
        if self._raw_mutation_token:
            self._mutation_token = MutationToken({
                'partition_id': self._raw_mutation_token.vbucket_id,
                'partition_uuid': self._raw_mutation_token.vbucket_uuid,
                'sequence_number': self._raw_mutation_token.seq_no,
                'bucket_name': self._raw_mutation_token.bucket_name,
            })
        

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._raw_result.get('cas', None)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._raw_result.get('key', None)

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
        return self._mutation_token

    def __repr__(self):
        output = {k: v for k, v in self._raw_result.items() if v is not None}
        output['mutation_token'] = self._mutation_token
        return 'MutationResult:{}'.format(output)

# Sub-document Operations

class ContentSubdocProxy:
    """
    Used to provide access to LookUpResult content via Result.content_as[type](index)
    """

    def __init__(self, content, key):
        self._content = content
        self._key = key

    def _parse_content_at_index(self, index, type_):
        if index > len(self._content) - 1 or index < 0:
            raise InvalidIndexException(
                f"Provided index ({index}) is invalid.")

        item = self._content[index].get('value', None)
        if item is None:
            status = self._content[index].get('status', None)
            if not status:
                # @TODO(jc):  is this correct?
                raise DocumentNotFoundException(
                    f"Could not find document for key: {self._key}")

            if status.code == StatusCode.UNKNOWN.value[0]:
                raise Exception(f"Unknown problem occured.  key: {self._key} index: {index}")
        #     # if status == SubDocStatus.PathNotFound:
        #     #     path = self._content[index].get("path", None)
        #     #     raise PathNotFoundException(
        #     #         f"Path ({path}) could not be found for key: {self._key}")
        #     # if status == SubDocStatus.PathMismatch:
        #     #     path = self._content[index].get("path", None)
        #     #     raise PathMismatchException(
        #     #         f"Path ({path}) mismatch for key: {self._key}")
        #     # if status == SubDocStatus.ValueCannotInsert:
        #     #     path = self._content[index].get("path", None)
        #     #     raise SubdocCantInsertValueException(
        #     #         f"Cannot insert value at path ({path}) for key: {self._key}")
        #     # if status == SubDocStatus.PathExists:
        #     #     path = self._content[index].get("path", None)
        #     #     raise PathExistsException(
        #     #         f"Path ({path}) already exists for key: {self._key}")

        return type_(item)

    def __getitem__(self,
                    type_       # type: Any
                    ) -> Any:
        """

        :param type_: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return lambda index: self._parse_content_at_index(index, type_)

class LookupInResult(LookupInResultInterface):
    def __init__(self,
                raw_result # type: Dict[str, Any]
        ):
        self._raw_result = raw_result

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._raw_result.get('cas', None)

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a dict::

                res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
                value = res.content_as[dict](0)
        """
        return ContentSubdocProxy(self.value, self.key)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._raw_result.get('key', None)

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
        return self._raw_result.get('value', None)

    def exists(self,
               index  # type: int
               ) -> bool:
        """Check if the subdocument path exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the provided index is out of range.

        Returns:
            bool: True if the path exists.  False if the path does not exist.
        """

        if index > len(self.value) - 1 or index < 0:
            raise InvalidIndexException(
                f"Provided index ({index}) is invalid.")

        # @TODO(JC):  how do we handle exists??
        exists = self.value[index].get('exists', None)
        return exists is not None and exists is True

    def __repr__(self):
        output = {k: v for k, v in self._raw_result.items() if v is not None}
        return 'LookupInResult:{}'.format(output)


class MutateInResult(MutateInResultInterface):
    def __init__(self,
                raw_result # type: Dict[str, Any]
        ):
        self._raw_result = raw_result
        self._raw_mutation_token = self._raw_result.get('mutation_token', None)
        self._mutation_token = None
        if self._raw_mutation_token:
            self._mutation_token = MutationToken({
                'partition_id': self._raw_mutation_token.vbucket_id,
                'partition_uuid': self._raw_mutation_token.vbucket_uuid,
                'sequence_number': self._raw_mutation_token.seq_no,
                'bucket_name': self._raw_mutation_token.bucket_name,
            })

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._raw_result.get('cas', None)

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a str::

                res = collection.mutate_in(key, (SD.upsert("city", "New City"),
                                                SD.replace("faa", "CTY")))
                value = res.content_as[str](0)
        """
        return ContentSubdocProxy(self.value, self.key)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._raw_result.get('flags', None)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._raw_result.get('key', None)

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
        output = {k: v for k, v in self._raw_result.items() if v is not None}
        output['mutation_token'] = self._mutation_token
        return 'MutateInResult:{}'.format(output)

"""

Python SDK Streaming Results

"""

class QueryResult(QueryResultInterface):
    def __init__(self,
                 n1ql_request # type: N1QLRequest
    ):
        self._request = n1ql_request

    def rows(self):
        """The rows which have been returned by the query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        return self.__iter__()

    def execute(self):
        """Convenience method to execute the query.

        Returns:
            List[Any]:  A list of query results.

        Example:
            q_rows = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;').execute()

        """
        return self._request.execute()

    def metadata(self):
        """The meta-data which has been returned by the query.

        Returns:
            :class:`~couchbase.n1ql.QueryMetaData`: An instance of :class:`~couchbase.n1ql.QueryMetaData`.
        """
        return self._request.metadata()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        raise NotImplementedError('Not an async QueryResult.')

    def __repr__(self):
        return "QueryResult:{}".format(self._request)