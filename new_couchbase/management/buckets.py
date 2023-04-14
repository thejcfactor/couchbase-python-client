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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Union)

from new_couchbase.api import ApiImplementation
from new_couchbase.common.management.buckets import BucketType  # noqa: F401
from new_couchbase.common.management.buckets import CompressionMode  # noqa: F401
from new_couchbase.common.management.buckets import ConflictResolutionType  # noqa: F401
from new_couchbase.common.management.buckets import EjectionMethod  # noqa: F401
from new_couchbase.common.management.buckets import EvictionPolicyType  # noqa: F401
from new_couchbase.common.management.buckets import StorageBackend  # noqa: F401
from new_couchbase.common.management.buckets import BucketSettings, CreateBucketSettings

# @TODO:  lets deprecate import of options from couchbase.management.buckets
from new_couchbase.management.options import (CreateBucketOptions,
                                              DropBucketOptions,
                                              FlushBucketOptions,
                                              GetAllBucketOptions,
                                              GetBucketOptions,
                                              UpdateBucketOptions)

if TYPE_CHECKING:
    from new_couchbase.classic.cluster import Cluster as ClassicCluster
    from new_couchbase.protostellar.cluster import Cluster as ProtostellarCluster


class BucketManager:

    def __init__(self,
                 cluster,  # type: Union[ClassicCluster, ProtostellarCluster]
                 ):
        if cluster.api_implementation == ApiImplementation.PROTOSTELLAR:
            from new_couchbase.protostellar.management.buckets import BucketManager
            self._impl = BucketManager(cluster)
        else:
            from new_couchbase.classic.management.buckets import BucketManager
            self._impl = BucketManager(cluster)

    def create_bucket(self,
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Dict[str, Any]
                      ) -> None:
        """Creates a new bucket.

        Args:
            settings (:class:`.CreateBucketSettings`): The settings to use for the new bucket.
            options (:class:`~couchbase.management.options.CreateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketAlreadyExistsException`: If the bucket already exists.
            :class:`~couchbase.exceptions.InvalidArgumentsException`: If an invalid type or value is provided for the
                settings argument.
        """
        self._impl.create_bucket(settings, *options, **kwargs)

    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Dict[str, Any]
                    ) -> None:
        """Drops an existing bucket.

        Args:
            bucket_name (str): The name of the bucket to drop.
            options (:class:`~couchbase.management.options.DropBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        self._impl.drop_bucket(bucket_name, *options, **kwargs)
    
    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Dict[str, Any]
                     ) -> None:
        """Flushes the bucket, deleting all the existing data that is stored in it.

        Args:
            bucket_name (str): The name of the bucket to flush.
            options (:class:`~couchbase.management.options.FlushBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
            :class:`~couchbase.exceptions.BucketNotFlushableException`: If the bucket's settings have
                flushing disabled.
        """
        self._impl.flush_bucket(bucket_name, *options, **kwargs)

    
    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Dict[str, Any]
                        ) -> List[BucketSettings]:
        """Returns a list of existing buckets in the cluster.

        Args:
            options (:class:`~couchbase.management.options.GetAllBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            List[:class:`.BucketSettings`]: A list of existing buckets in the cluster.
        """
        return self._impl.get_all_buckets(*options, **kwargs)
    
    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Dict[str, Any]
                   ) -> BucketSettings:
        """Fetches the settings in use for a specified bucket.

        Args:
            bucket_name (str): The name of the bucket to fetch.
            options (:class:`~couchbase.management.options.GetBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Returns:
            :class:`.BucketSettings`: The settings of the specified bucket.

        Raises:
            :class:`~couchbase.exceptions.BucketDoesNotExistException`: If the bucket does not exist.
        """
        return self._impl.get_bucket(bucket_name, *options, **kwargs)
    
    def update_bucket(self,
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Dict[str, Any]
                      ) -> None:
        """Update the settings for an existing bucket.

        Args:
            settings (:class:`.BucketSettings`): The settings to use for the new bucket.
            options (:class:`~couchbase.management.options.UpdateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentsException`: If an invalid type or value is provided for the
                settings argument.
        """
        self._impl.update_bucket(settings, *options, **kwargs)
