#  Copyright 2016-2022. Couchbase, Inc.
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
from typing import (Any,
                    Dict,
                    List,
                    Tuple,
                    Union,
                    TYPE_CHECKING)

from new_couchbase.exceptions import MissingTokenException

if TYPE_CHECKING:
    from new_couchbase.result import MutationResult

class MutationToken:
    def __init__(self, token  # type: Dict[str, Union[str, int]]
                 ):
        self._token = token

    @property
    def partition_id(self) -> int:
        """
            int:  The token's partition id.
        """
        return self._token['partition_id']

    @property
    def partition_uuid(self) -> int:
        """
            int:  The token's partition uuid.
        """
        return self._token['partition_uuid']

    @property
    def sequence_number(self) -> int:
        """
            int:  The token's sequence number.
        """
        return self._token['sequence_number']

    @property
    def bucket_name(self) -> str:
        """
            str:  The token's bucket name.
        """
        return self._token['bucket_name']

    def as_tuple(self) -> Tuple[int, int, int, str]:
        return (self.partition_id, self.partition_uuid,
                self.sequence_number, self.bucket_name)

    def as_dict(self) -> Dict[str, Union[str, int]]:
        return self._token

    def __repr__(self):
        return "MutationToken:{}".format(self._token)

    def __hash__(self):
        return hash(self.as_tuple())

    def __eq__(self, other):
        if not isinstance(other, MutationToken):
            return False
        return (self.partition_id == other.partition_id
                and self.partition_uuid == other.partition_uuid
                and self.sequence_number == other.sequence_number
                and self.bucket_name == other.bucket_name)

class MutationState:
    def __init__(self, *docs,  # type: List[MutationResult]
                 **kwargs  # type: Dict[str, Any]
                 ):
        self._sv = set()
        if docs:
            self.add_results(*docs, **kwargs)

    def add_mutation_token(self, mut_token  # type: MutationToken
                           ) -> None:
        if isinstance(mut_token, MutationToken):
            self._sv.add(mut_token)

    def _add_scanvec(self, mut_token  # type: MutationToken
                     ) -> bool:
        """
        Internal method used to specify a scan vector.
        :param mut_token: A tuple in the form of
            `(vbucket id, vbucket uuid, mutation sequence)`
        """
        if isinstance(mut_token, MutationToken):
            self._sv.add(mut_token)
            return True

        return False

    def add_results(self, *rvs,  # type: List[MutationResult]
                    **kwargs  # type: Dict[str, Any]
                    ) -> bool:
        """
        Changes the state to reflect the mutation which yielded the given
        result.

        In order to use the result, the `enable_mutation_tokens` option must
        have been specified in the connection string, _and_ the result
        must have been successful.

        :param rvs: One or more :class:`~.OperationResult` which have been
            returned from mutations
        :param quiet: Suppress errors if one of the results does not
            contain a convertible state.
        :return: `True` if the result was valid and added, `False` if not
            added (and `quiet` was specified
        :raise: :exc:`~.MissingTokenException` if `result` does not contain
            a valid token
        """
        if not rvs:
            raise MissingTokenException(message='No results passed')
        for rv in rvs:
            mut_token = rv.mutation_token()
            if not isinstance(mut_token, MutationToken):
                if kwargs.get('quiet', False) is True:
                    return False
                raise MissingTokenException(
                    message='Result does not contain token')
            if not self._add_scanvec(mut_token):
                return False
        return True

    def add_all(self, bucket, quiet=False):
        """
        Ensures the query result is consistent with all prior
        mutations performed by a given bucket.

        Using this function is equivalent to keeping track of all
        mutations performed by the given bucket, and passing them to
        :meth:`~add_result`

        :param bucket: A :class:`~couchbase_core.client.Client` object
            used for the mutations
        :param quiet: If the bucket contains no valid mutations, this
            option suppresses throwing exceptions.
        :return: `True` if at least one mutation was added, `False` if none
            were added (and `quiet` was specified)
        :raise: :exc:`~.MissingTokenException` if no mutations were added and
            `quiet` was not specified
        """
        raise NotImplementedError("Feature currently not implemented in 4.x series of the Python SDK")

    def __repr__(self):
        return "MutationState:{}".format(self._token)