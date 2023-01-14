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

from enum import IntEnum


class Authenticator(dict):
    pass

class AuthDomain(IntEnum):
    """
    The Authentication domain for a user.

    Local: Users managed by Couchbase Server.
    External: Users managed by an external resource, eg LDAP.
    """
    Local = 0
    External = 1

    @classmethod
    def to_str(cls, value):
        if value == cls.External:
            return "external"
        else:
            return "local"

    @classmethod
    def from_str(cls, value):
        if value == "external":
            return cls.External
        else:
            return cls.Local