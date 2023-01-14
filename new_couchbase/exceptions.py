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
                message=None, # type: Optional[str]
                **kwargs, # type: Dict[str, Any]
                ):
        self.message=message


class FeatureUnavailableException(CouchbaseException):
    """Raised when feature that is not available with the current server version is used."""


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
