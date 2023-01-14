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

import warnings


class CouchbaseDeprecationWarning(UserWarning):
    """
    Couchbase Python SDK Warning Category
    """


class Supportability:
    @classmethod
    def import_deprecated(cls, old_path, new_path):
        def decorator(cls):
            old_init = cls.__init__

            def new_init(self, *args, **kwargs):
                msg = (f"Importing {cls.__name__} from {old_path} is deprecated "
                       "and will be removed in a future release. "
                       f" Import {cls.__name__} from {new_path} instead.")
                warnings.warn(msg, CouchbaseDeprecationWarning, stacklevel=2)
                old_init(self, *args, **kwargs)

            cls.__init__ = new_init
            return cls
        return decorator