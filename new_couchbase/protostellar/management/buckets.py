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

from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Tuple,
                    TYPE_CHECKING)

from new_couchbase.common.management.buckets import BucketSettings

from new_couchbase.common.options import parse_options
from new_couchbase.common._utils import (is_null_or_empty,
                                         enum_to_str,
                                         str_to_enum,
                                         to_seconds,
                                         validate_bool,
                                         validate_int,
                                         validate_str)
from new_couchbase.common.management.buckets import (BucketType,
                                                     CompressionMode,
                                                     ConflictResolutionType,
                                                     EvictionPolicyType,
                                                     StorageBackend)
from new_couchbase.common.management.options import BucketMgmtOptionTypes
from new_couchbase.classic.management.options import ValidBucketMgmtOptions
from new_couchbase.exceptions import BucketDoesNotExistException, FeatureUnavailableException, InvalidArgumentException
from new_couchbase.protostellar import bucket_pb2_grpc as bucket_mgmt
from new_couchbase.protostellar.proto.couchbase.admin.bucket.v1 import bucket_pb2
from new_couchbase.protostellar.durability import durability_level_from_protostellar, to_protostellar_durability_level
from new_couchbase.protostellar.result import ProtostellarResponse

from new_couchbase.protostellar.management.wrappers import BlockingMgmtWrapper, ManagementType

if TYPE_CHECKING:
    from new_couchbase.protostellar.cluster import Cluster
    from new_couchbase.common.management.buckets import CreateBucketSettings
    from new_couchbase.management.options import (CreateBucketOptions,
                                                  DropBucketOptions,
                                                  FlushBucketOptions,
                                                  GetAllBucketOptions,
                                                  GetBucketOptions,
                                                  UpdateBucketOptions)


class BucketManager:
    def __init__(self,
                 cluster # type: Cluster
                 ):
        self._cluster = cluster
        self._bucket_svc = bucket_mgmt.BucketAdminServiceStub(self._cluster.connection)

    @property
    def metadata(self) -> List[Tuple[str]]:
        """
        **INTERNAL**
        """
        return self._cluster.metadata

    def _get_call_args(self, 
                      **options, # type: Dict[str, Any]
                      ) -> Dict[str, Any]:
        timeout = options.pop('timeout', None)
        call_args = {
            'metadata': self.metadata
        }
        if timeout:
            call_args['timeout'] = timeout

        return call_args

    @BlockingMgmtWrapper.block(None, ManagementType.BucketMgmt, None)
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
        req_args = self._to_protostellar_bucket_settings(settings)
        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.Create)
        final_args = parse_options(valid_opts, kwargs, *options)
        call_args = self._get_call_args(**final_args)
        req = bucket_pb2.CreateBucketRequest(**req_args)
        print(req)
        response, call = self._bucket_svc.CreateBucket.with_call(req, **call_args)
        return ProtostellarResponse(response, call, None, None)


    @BlockingMgmtWrapper.block(None, ManagementType.BucketMgmt, None)
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
        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.Drop)
        final_args = parse_options(valid_opts, kwargs, *options)
        call_args = self._get_call_args(**final_args)
        req_args = {'bucket_name': bucket_name}
        req = bucket_pb2.DeleteBucketRequest(**req_args)
        response, call = self._bucket_svc.DeleteBucket.with_call(req, **call_args)
        return ProtostellarResponse(response, call, None, None)
    
    @BlockingMgmtWrapper.block(None, ManagementType.BucketMgmt, None)
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
        raise FeatureUnavailableException('Protostellar does not support bucket flush atm.')

    @BlockingMgmtWrapper.block(list, ManagementType.BucketMgmt, None)
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
        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.GetAll)
        final_args = parse_options(valid_opts, kwargs, *options)
        call_args = self._get_call_args(**final_args)
        req = bucket_pb2.ListBucketsRequest()
        response, call = self._bucket_svc.ListBuckets.with_call(req, **call_args)
        return ProtostellarResponse(response, call, None, None)
    
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
        bucket_list = self.get_all_buckets(*options, **kwargs)
        match = next((b for b in bucket_list if b.name == bucket_name), None)
        if not match:
            raise BucketDoesNotExistException(f'Unable to find bucket with name: {bucket_name}')
        
        return match

    
    @BlockingMgmtWrapper.block(None, ManagementType.BucketMgmt, None)
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
        req_args = self._to_protostellar_bucket_settings(settings, is_update=True)
        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.Update)
        final_args = parse_options(valid_opts, kwargs, *options)
        call_args = self._get_call_args(**final_args)
        req = bucket_pb2.UpdateBucketRequest(**req_args)
        print(req)
        response, call = self._bucket_svc.UpdateBucket.with_call(req, **call_args)
        return ProtostellarResponse(response, call, None, None)

    def _to_protostellar_bucket_settings(self,
                                         settings, # type: Dict[str, Any]
                                         is_update=False, # type: Optional[bool]
                                         ) -> Dict[str, Any]:
        """**INTERNAL**"""
        output = dict()
        if not is_update:
            bucket_type = settings.get('bucket_type', BucketType.COUCHBASE)
            output['bucket_type'] = self._to_protostellar_bucket_type(bucket_type)
        compression_mode = settings.get('compression_mode', None)
        if compression_mode:
            output['compression_mode'] = self._to_protostellar_compression_mode(compression_mode)
        conflict_resolution_type = settings.get('conflict_resolution_type', None)
        if conflict_resolution_type:
            output['conflict_resolution_type'] = self._to_protostellar_conflict_resolution_type(conflict_resolution_type)
        eviction_policy = settings.get('eviction_policy', None)
        if eviction_policy:
            output['eviction_mode'] = self._to_protostellar_eviction_policy(eviction_policy)
        output['flush_enabled'] = settings.get('flush_enabled', False)
        max_expiry = settings.get('max_expiry', None)
        if max_expiry:
            output['max_expiry_secs'] = to_seconds(max_expiry)
        minimum_durability_level = settings.get('minimum_durability_level', None)
        if minimum_durability_level:
            output['minimum_durability_level'] = to_protostellar_durability_level(minimum_durability_level)
        name = settings.get('name', None)
        if name:
            output['bucket_name'] = validate_str(name)
        num_replicas = settings.get('num_replicas', 0)
        output['num_replicas'] = validate_int(num_replicas)
        ram_quota_mb = settings.get('ram_quota_mb', 100)
        if ram_quota_mb:
            valid_mb = validate_int(ram_quota_mb)
            output['ram_quota_bytes'] = valid_mb * 1024 * 1024
        replica_index = settings.get('replica_index', None)
        if replica_index:
            output['replica_indexes'] = validate_bool(replica_index)
        if not is_update:
            storage_backend = settings.get('storage_backend', StorageBackend.COUCHSTORE)
            output['storage_backend'] = self._to_protostellar_storage_backend(storage_backend)
        return output

    @staticmethod
    def bucket_settings_from_protostellar(bucket, # type: bucket_pb2.ListBucketsResponse.Bucket
                                          ) -> BucketSettings:
        """**INTERNAL**"""
        output = dict()
        output['bucket_type'] = BucketManager.bucket_type_from_protostellar(bucket.bucket_type)
        output['compression_mode'] = BucketManager.compression_mode_from_protostellar(bucket.compression_mode)
        cft = BucketManager.conflict_resolution_type_from_protostellar(bucket.conflict_resolution_type)
        output['conflict_resolution_type'] = cft
        output['eviction_policy'] = BucketManager.eviction_policy_from_protostellar(bucket.eviction_mode)
        output['flush_enabled'] = bucket.flush_enabled
        output['max_expiry'] = bucket.max_expiry_secs
        output['minimum_durability_level'] = durability_level_from_protostellar(bucket.minimum_durability_level)
        output['name'] = bucket.bucket_name
        output['num_replicas'] = bucket.num_replicas
        output['ram_quota_mb'] = bucket.ram_quota_bytes / 1024 / 1024
        output['replica_index'] = bucket.replica_indexes
        output['storage_backend'] = BucketManager.storage_backend_from_protostellar(bucket.storage_backend)
        return BucketSettings(**output)
    
    def _to_protostellar_bucket_type(self,
                                     value # type: BucketType
                                    ) -> bucket_pb2.BucketType:
        if value == BucketType.COUCHBASE:
            return bucket_pb2.BucketType.BUCKET_TYPE_COUCHBASE
        elif value == BucketType.EPHEMERAL:
            return bucket_pb2.BucketType.BUCKET_TYPE_EPHEMERAL
        elif value == BucketType.MEMCACHED:
            return bucket_pb2.BucketType.BUCKET_TYPE_MEMCACHED
        else:
            raise InvalidArgumentException(f"Argument must be of type BucketType but got {type(value)}.")
        
    @staticmethod
    def bucket_type_from_protostellar(value # type: bucket_pb2.BucketType
                                       ) -> BucketType:
        if value == bucket_pb2.BucketType.BUCKET_TYPE_COUCHBASE:
            return BucketType.COUCHBASE
        elif value == bucket_pb2.BucketType.BUCKET_TYPE_EPHEMERAL:
            return BucketType.EPHEMERAL
        elif value == bucket_pb2.BucketType.BUCKET_TYPE_MEMCACHED:
            return BucketType.MEMCACHED
        else:
            raise InvalidArgumentException(f"Argument must be of type BucketType but got {type(value)}.")

    def _to_protostellar_compression_mode(self,
                                          value # type: CompressionMode
                                          ) -> bucket_pb2.CompressionMode:
        if value == CompressionMode.ACTIVE:
            return bucket_pb2.CompressionMode.COMPRESSION_MODE_ACTIVE
        elif value == CompressionMode.OFF:
            return bucket_pb2.CompressionMode.COMPRESSION_MODE_OFF
        elif value == CompressionMode.PASSIVE:
            return bucket_pb2.CompressionMode.COMPRESSION_MODE_PASSIVE
        else:
            raise InvalidArgumentException(f"Argument must be of type CompressionMode but got {type(value)}.")
    
    @staticmethod
    def compression_mode_from_protostellar(value # type: bucket_pb2.CompressionMode
                                            ) -> CompressionMode:
        if value == bucket_pb2.CompressionMode.COMPRESSION_MODE_ACTIVE:
            return CompressionMode.ACTIVE
        elif value == bucket_pb2.CompressionMode.COMPRESSION_MODE_OFF:
            return CompressionMode.OFF
        elif value == bucket_pb2.CompressionMode.COMPRESSION_MODE_PASSIVE:
            return CompressionMode.PASSIVE
        else:
            raise InvalidArgumentException(f"Argument must be of type CompressionMode but got {type(value)}.")

    def _to_protostellar_conflict_resolution_type(self,
                                         value # type: ConflictResolutionType
                                         ) -> bucket_pb2.ConflictResolutionType:
        if value == ConflictResolutionType.CUSTOM:
            return bucket_pb2.ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_CUSTOM
        elif value == ConflictResolutionType.SEQUENCE_NUMBER:
            return bucket_pb2.ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER
        elif value == ConflictResolutionType.TIMESTAMP:
            return bucket_pb2.ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_TIMESTAMP
        else:
            raise InvalidArgumentException(f"Argument must be of type ConflictResolutionType but got {type(value)}.")
        
    @staticmethod
    def conflict_resolution_type_from_protostellar(value # type: bucket_pb2.ConflictResolutionType
                                                    ) -> ConflictResolutionType:
        if value == bucket_pb2.ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_CUSTOM:
            return ConflictResolutionType.CUSTOM
        elif value == bucket_pb2.ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER:
            return ConflictResolutionType.SEQUENCE_NUMBER
        elif value == bucket_pb2.ConflictResolutionType.CONFLICT_RESOLUTION_TYPE_TIMESTAMP:
            return ConflictResolutionType.TIMESTAMP
        else:
            raise InvalidArgumentException(f"Argument must be of type ConflictResolutionType but got {type(value)}.")

    def _to_protostellar_eviction_policy(self,
                                         value # type: EvictionPolicyType
                                         ) -> bucket_pb2.EvictionMode:
        if value == EvictionPolicyType.FULL:
            return bucket_pb2.EvictionMode.EVICTION_MODE_FULL
        elif value == EvictionPolicyType.NO_EVICTION:
            return bucket_pb2.EvictionMode.EVICTION_MODE_NONE
        elif value == EvictionPolicyType.NOT_RECENTLY_USED:
            return bucket_pb2.EvictionMode.EVICTION_MODE_NOT_RECENTLY_USED
        elif value == EvictionPolicyType.VALUE_ONLY:
            return bucket_pb2.EvictionMode.EVICTION_MODE_VALUE_ONLY
        else:
            raise InvalidArgumentException(f"Argument must be of type EvictionPolicyType but got {type(value)}.")

    @staticmethod
    def eviction_policy_from_protostellar(value # type: bucket_pb2.EvictionMode
                                           ) -> EvictionPolicyType:
        if value == bucket_pb2.EvictionMode.EVICTION_MODE_FULL:
            return EvictionPolicyType.FULL
        elif value == bucket_pb2.EvictionMode.EVICTION_MODE_NONE:
            return EvictionPolicyType.NO_EVICTION
        elif value == bucket_pb2.EvictionMode.EVICTION_MODE_NOT_RECENTLY_USED:
            return EvictionPolicyType.NOT_RECENTLY_USED
        elif value == bucket_pb2.EvictionMode.EVICTION_MODE_VALUE_ONLY:
            return EvictionPolicyType.VALUE_ONLY
        else:
            raise InvalidArgumentException(f"Argument must be of type EvictionPolicyType but got {type(value)}.")

    def _to_protostellar_storage_backend(self,
                                         value # type: StorageBackend
                                         ) -> bucket_pb2.StorageBackend:
        if value == StorageBackend.COUCHSTORE:
            return bucket_pb2.StorageBackend.STORAGE_BACKEND_COUCHSTORE
        elif value == StorageBackend.MAGMA:
            return bucket_pb2.StorageBackend.STORAGE_BACKEND_MAGMA
        elif value == StorageBackend.UNDEFINED:
            raise InvalidArgumentException("Protostellar does not support StorageBackend.UNDEFINED")
        else:
            raise InvalidArgumentException(f"Argument must be of type StorageBackend but got {type(value)}.")

    @staticmethod
    def storage_backend_from_protostellar(value # type: bucket_pb2.StorageBackend
                                         ) -> StorageBackend:
        if value == bucket_pb2.StorageBackend.STORAGE_BACKEND_COUCHSTORE:
            return StorageBackend.COUCHSTORE
        elif value == bucket_pb2.StorageBackend.STORAGE_BACKEND_MAGMA:
            return StorageBackend.MAGMA
        else:
            raise InvalidArgumentException(f"Argument must be of type StorageBackend but got {type(value)}.")