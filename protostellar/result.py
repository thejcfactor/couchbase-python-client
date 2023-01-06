from datetime import datetime
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional,
                    Tuple,
                    Union)

from protostellar._utils import timestamp_as_datetime
from protostellar.proto.couchbase.kv import v1_pb2

from couchbase.exceptions import InvalidIndexException
from protostellar.transcoder import JSONTranscoder
<<<<<<< HEAD

from couchbase.result import QueryResult  # noqa: F401
=======
>>>>>>> 0b9b09c (updates)

if TYPE_CHECKING:
    from protostellar.transcoder import Transcoder


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


class ExistsResult:

    def __init__(self,
                 key,  # type: str
                 response,  # type:  v1_pb2.ExistsResponse
                 ):
        self._key = key
        self._cas = response.cas
        self._exists = response.result

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._key

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._cas

    @property
    def exists(self) -> bool:
        """
            bool: True if the document exists, false otherwise.
        """
        return self._exists

    def __repr__(self):
        output = {
            'key': self._key,
            'cas': self._cas,
            'exists': self._exists,
        }
        return "ExistsResult:{}".format(output)


class GetResult:

    def __init__(self,
                 key,  # type: str
                 response,  # type:  v1_pb2.GetResponse
                 transcoder,  # type: Transcoder
                 ):
        self._key = key
        self._cas = response.cas
        self._expiry = None
        if response.HasField('expiry'):
            self._expiry = timestamp_as_datetime(response.expiry)

        self._content = transcoder.decode_value(response.content, response.content_type)

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._cas

    @property
    def expiry_time(self) -> Optional[datetime]:
        """
            Optional[datetime]: The expiry of the document, if it was requested.
        """
        # if time_ms:
        #     return datetime.fromtimestamp(time_ms)
        return self._expiry

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get(key)
                value = res.content_as[dict]

        """
        return ContentProxy(self._content)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._key

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas != 0

    def __repr__(self):
        output = {
            'key': self._key,
            'cas': self._cas,
            'value': self._content,
        }
        if self._expiry:
            output['expiry_time'] = self._expiry

        return "GetResult:{}".format(output)


class MutationToken:
    def __init__(self, token  # type: Dict[str, Union[str, int]]
                 ):
        self._token = token

    @property
    def partition_id(self) -> int:
        """
            int:  The token's partition id.
        """
        return self._token.vbucket_id

    @property
    def partition_uuid(self) -> int:
        """
            int:  The token's partition uuid.
        """
        return self._token.vbucket_uuid

    @property
    def sequence_number(self) -> int:
        """
            int:  The token's sequence number.
        """
        return self._token.seq_no

    @property
    def bucket_name(self) -> str:
        """
            str:  The token's bucket name.
        """
        return self._token.bucket_name

    def as_tuple(self) -> Tuple[int, int, int, str]:
        return (self.partition_id, self.partition_uuid,
                self.sequence_number, self.bucket_name)

    def as_dict(self) -> Dict[str, Union[str, int]]:
        return self._token

    def __repr__(self):
        output = {
            'bucket_name': self.bucket_name,
            'partition_id': self.partition_id,
            'partition_uuid': self.partition_uuid,
            'sequence_number': self.sequence_number,
        }
        return "MutationToken:{}".format(output)

    def __hash__(self):
        return hash(self.as_tuple())

    def __eq__(self, other):
        if not isinstance(other, MutationToken):
            return False
        return (self.partition_id == other.partition_id
                and self.partition_uuid == other.partition_uuid
                and self.sequence_number == other.sequence_number
                and self.bucket_name == other.bucket_name)


class MutationResult:

    def __init__(self,
                 key,  # type: str
                 response,  # type: v1_pb2.UpsertResponse
                 ):

        self._key = key
        self._cas = response.cas
        self._mutation_token = MutationToken(response.mutation_token)

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._cas

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._key

    @property
    def mutation_token(self) -> MutationToken:
        """Get the operation's mutation token.

        Returns:
            :class:`.MutationToken`: The operation's mutation token.
        """
        return self._mutation_token

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas != 0

    def __repr__(self):
        output = {
            'key': self._key,
            'cas': self._cas,
            'mutation_token': self._mutation_token,
        }

        return "MutationResult:{}".format(output)

class ContentSubdocProxy:
    """
    Used to provide access to LookUpResult content via Result.content_as[type](index)
    """

    def __init__(self, content):
        self._content = content

    def _parse_content_at_index(self, index, type_):
        if index > len(self._content) - 1 or index < 0:
            raise InvalidIndexException(
                f"Provided index ({index}) is invalid.")

        item = self._content[index]
        if item is None:
            # TODO: how to handle
            raise ValueError(f'Unable to parse content at index: {index}')

        return type_(item)

    def __getitem__(self,
                    type_       # type: Any
                    ) -> Any:
        """

        :param type_: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return lambda index: self._parse_content_at_index(index, type_)

class LookupInResult:

    def __init__(self, 
                response # type: v1_pb2.LookupInResponse
            ):

        self._cas = response.cas
        self._specs = []
        tc = JSONTranscoder()
        for spec in response.specs:
            # print(spec)
            self._specs.append(tc.decode_value(spec.content, v1_pb2.JSON))

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._cas

    def exists(self,  # type: LookupInResult
               index  # type: int
               ) -> bool:
        """Check if the subdocument path exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the provided index is out of range.

        Returns:
            bool: True if the path exists.  False if the path does not exist.
        """

        if index > len(self._specs) - 1 or index < 0:
            raise InvalidIndexException(
                f"Provided index ({index}) is invalid.")

        exists = self._specs[index]
        return exists is not None and exists is True

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a dict::

                res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
                value = res.content_as[dict](0)
        """
        return ContentSubdocProxy(self._specs)

    def __repr__(self):
        output = {
            'cas': self._cas,
            'specs': self._specs
        }
        return "LookupInResult:{}".format(output)

class MutateInResult:

    def __init__(self, 
                response # type: v1_pb2.MutateInResponse
            ):

        self._cas = response.cas
        self._mutation_token = MutationToken(response.mutation_token)
        self._specs = []
        tc = JSONTranscoder()
        for spec in response.specs:
            if spec.HasField('content'):
                self._specs.append(tc.decode_value(spec.content, v1_pb2.JSON))

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._cas

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a dict::

                res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
                value = res.content_as[dict](0)
        """
        return ContentSubdocProxy(self._specs)

    @property
    def mutation_token(self) -> MutationToken:
        """Get the operation's mutation token.

        Returns:
            :class:`.MutationToken`: The operation's mutation token.
        """
        return self._mutation_token

    def __repr__(self):
        output = {
            'cas': self._cas,
            'mutation_token': self._mutation_token,
            'specs': self._specs
        }
        return "MutateInResult:{}".format(output)
