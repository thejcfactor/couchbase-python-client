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
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional,
                    Union)

class CouchbaseException(Exception):
    def __init__(self,
                 base=None,  # type: Optional[Any]
                 message=None,     # type: Optional[str]
                 context=None,      # type: Optional[Any]
                 error_code=None,  # type: Optional[int]
                 exc_info=None,      # type: Optional[Dict[str, Any]]
                **kwargs, # type: Dict[str, Any]
                ):
        # @TODO(jc): base, context and error codes
        # self._base = base
        # self._context = context
        self._message = message
        # self._error_code = error_code
        self._exc_info = exc_info
        super().__init__(message)

    def __str__(self):
        from new_couchbase.common._utils import is_null_or_empty
        details = []
        if not is_null_or_empty(self._message):
            details.append("message={}".format(self._message))
        # if self._base:
        #     details.append(
        #         "ec={}, category={}".format(
        #             self._base.err(),
        #             self._base.err_category()))
        #     if not is_null_or_empty(self._message):
        #         details.append("message={}".format(self._message))
        #     else:
        #         details.append("message={}".format(self._base.strerror()))
        # else:
        #     if not is_null_or_empty(self._message):
        #         details.append(f'message={self._message}')
        # if self._context:
        #     details.append(f'context={self._context}')
        # if self._exc_info and 'cinfo' in self._exc_info:
        #     details.append('C Source={0}:{1}'.format(*self._exc_info['cinfo']))
        if self._exc_info and 'inner_cause' in self._exc_info:
            details.append('Inner cause={0}'.format(self._exc_info['inner_cause']))
        return "<{}>".format(", ".join(details))


class AmbiguousTimeoutException(CouchbaseException):
    """ AmbiguousTimeoutException """

class CasMismatchException(CouchbaseException):
    pass

CASMismatchException = CasMismatchException

class DurabilityImpossibleException(CouchbaseException):
    """Given durability requirements are impossible to achieve"""

class FeatureUnavailableException(CouchbaseException):
    """Raised when feature that is not available with the current server version is used."""
    def __init__(self, msg=None, **kwargs):
        if msg:
            kwargs['message'] = msg
        super().__init__(**kwargs)

class HTTPException(CouchbaseException):
    """HTTP error"""

class InternalSDKException(CouchbaseException):
    """
    This means the SDK has done something wrong. Get support.
    (this doesn't mean *you* didn't do anything wrong, it does mean you should
    not be seeing this message)
    """

    def __init__(self, msg=None, **kwargs):
        if msg:
            kwargs['message'] = msg
        super().__init__(**kwargs)

class InvalidArgumentException(CouchbaseException):
    """ Raised when a provided argmument has an invalid value
        and/or invalid type.
    """

    def __init__(self, msg=None, **kwargs):
        if msg:
            kwargs['message'] = msg
        super().__init__(**kwargs)

class MissingTokenException(CouchbaseException):
     """Raised when trying to add mutation token to mutation state and token does not exist."""

class ServiceUnavailableException(CouchbaseException):
    """ Raised if tt can be determined from the config unambiguously that a
        given service is not available.
        I.e. no query node in the config, or a memcached bucket is accessed
        and views or n1ql queries should be performed
    """

class TemporaryFailException(CouchbaseException):
    """Raised when the Server returns a temporary failure."""

class ValueFormatException(CouchbaseException):
    """Failed to decode or encode value"""

    def __init__(self, msg=None, **kwargs):
        if msg:
            kwargs['message'] = msg
        super().__init__(**kwargs)

class UnAmbiguousTimeoutException(CouchbaseException):
    """ UnAmbiguousTimeoutException """

"""

Python SDK Key-Value Exceptions

"""

class DocumentExistsException(CouchbaseException):
    """Indicates that the referenced document exists already, but the operation was not expecting it to exist."""

class DocumentLockedException(CouchbaseException):
    """Indicates that the referenced document could not be used as it is currently locked,
    likely by another actor in the system."""

class DocumentNotFoundException(CouchbaseException):
    """Indicates that the referenced document does not exist."""

class DocumentUnretrievableException(CouchbaseException):
    """Indicates that the referenced document does not exist and therefore no replicas are found."""

class InvalidIndexException(CouchbaseException):
    """Indicates the provided subdocument result index was invalid."""

class InvalidValueException(CouchbaseException):
    """Indicates the provided value was invalid for the operation."""

class PathExistsException(CouchbaseException):
    """Indicates that the reference path already existed, but the operation expected that it did not."""

class PathMismatchException(CouchbaseException):
    """Indicates that the referenced path made incorrect assumptions about the structure of a document,
    for instance attempting to access a field as an object when in fact it is an array."""

class PathNotFoundException(CouchbaseException):
    """Indicates that the reference path was not found."""

# @TODO:  How to Deprecate?
SubdocCantInsertValueException = InvalidValueException
SubdocPathMismatchException = PathMismatchException

"""

Python SDK Streaming Exceptions

"""

class AlreadyQueriedException(CouchbaseException):
    """
    Raised when query (N1QL, Search, Analytics or Views) results
    have already been iterated over.
    """

    def __init__(self, message='Previously iterated over results.'):
        super().__init__(message=message)

class KeyspaceNotFoundException(CouchbaseException):
    """Keyspace not found (collection or bucket does not exist)"""

class QueryIndexNotFoundException(CouchbaseException):
    """ The query index was not found"""

    def __init__(self, msg=None, **kwargs):
        if msg:
            kwargs['message'] = msg
        super().__init__(**kwargs)

class ScopeNotFoundException(CouchbaseException):
    """The scope was not found"""


"""

Python SDK Bucket Management Exceptions

"""

class BucketAlreadyExistsException(CouchbaseException):
    pass


class BucketDoesNotExistException(CouchbaseException):
    pass


class BucketNotFlushableException(CouchbaseException):
    pass