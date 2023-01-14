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

from typing import TYPE_CHECKING

from new_couchbase.api.bucket import BucketInterface

from new_couchbase.api.collection import CollectionInterface
from new_couchbase.api.serializer import SerializerInterface
from new_couchbase.api.scope import ScopeInterface
from new_couchbase.api.transcoder import TranscoderInterface
from new_couchbase.impl.server.core import BlockingWrapper
from new_couchbase.impl.server.exceptions import exception as BaseCouchbaseException
from new_couchbase.impl.server.exceptions import ErrorMapper
from new_couchbase.api import ApiImplementation

from new_couchbase.scope import Scope
from new_couchbase.collection import Collection

if TYPE_CHECKING:
    from new_couchbase.impl.server.core.bucket import BucketCore

class Bucket(BucketInterface):
    def __init__(self,
                 core: BucketCore
                 ) -> Bucket:
        self._core = core
        self._open_bucket()

    @property
    def api_implementation(self) -> ApiImplementation:
        return ApiImplementation.SERVER

    @property
    def connected(self) -> bool:
        return self._core.connected

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._core.connection

    @property
    def default_serializer(self) -> SerializerInterface:
        return self._core.default_serializer

    @property
    def default_transcoder(self) -> TranscoderInterface:
        return self._core.default_transcoder

    @property
    def name(self) -> str:
        return self._core.name

    @BlockingWrapper.block(True)
    def _close_bucket(self, **kwargs):
        self._core.open_or_close_bucket(open_bucket=False, **kwargs)
        self._core.destroy_connection()

    @BlockingWrapper.block(True)
    def _open_bucket(self, **kwargs):
        ret = self._core.open_or_close_bucket(open_bucket=True, **kwargs)
        if isinstance(ret, BaseCouchbaseException):
            raise ErrorMapper.build_exception(ret)
        self._core.set_connected(ret)

    def close(self):
        """Shuts down this bucket instance. Cleaning up all resources associated with it.

        .. warning::
            Use of this method is almost *always* unnecessary.  Bucket resources should be cleaned
            up once the bucket instance falls out of scope.  However, in some applications tuning resources
            is necessary and in those types of applications, this method might be beneficial.

        """
        # only close if we are connected
        if self.connected:
            self._close_bucket()

    def collection(self, 
                collection_name # type: str
                ) -> CollectionInterface:
        """Creates a :class:`~couchbase.collection.Collection` instance of the specified collection.

        Args:
            collection_name (str): Name of the collection to reference.

        Returns:
            :class:`~couchbase.collection.Collection`: A :class:`~couchbase.collection.Collection` instance of the specified collection.

        """  # noqa: E501
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self) -> CollectionInterface:
        """Creates a :class:`~couchbase.collection.Collection` instance of the default collection.

        Returns:
            :class:`~couchbase.collection.Collection`: A :class:`~couchbase.collection.Collection` instance of the default collection.
        """  # noqa: E501
        scope = self.default_scope()
        return scope.collection(Collection.default_name())

    def default_scope(self) -> ScopeInterface:
        """Creates a :class:`~.scope.Scope` instance of the default scope.

        Returns:
            :class:`~.scope.Scope`: A :class:`~.scope.Scope` instance of the default scope.

        """
        return self.scope(Scope.default_name())

    def scope(self, 
                scope_name # type: str
                ) -> ScopeInterface:
        """Creates a :class:`~couchbase.scope.Scope` instance of the specified scope.

        Args:
            name (str): Name of the scope to reference.

        Returns:
            :class:`~couchbase.scope.Scope`: A :class:`~couchbase.scope.Scope` instance of the specified scope.

        """
        return Scope(self, scope_name)