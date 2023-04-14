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

from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Union)

from new_couchbase.exceptions import (BucketAlreadyExistsException,
                                      BucketDoesNotExistException,
                                      BucketNotFlushableException,
                                      FeatureUnavailableException,
                                      InvalidArgumentException)
from new_couchbase.common._utils import (is_null_or_empty,
                                         enum_to_str,
                                         str_to_enum,
                                         to_seconds,
                                         validate_bool,
                                         validate_int,
                                         validate_str)
from new_couchbase.common.options import parse_options
from new_couchbase.common.management.buckets import (BucketType,
                                                     CompressionMode,
                                                     ConflictResolutionType,
                                                     EvictionPolicyType,
                                                     StorageBackend)
from new_couchbase.common.management.options import BucketMgmtOptionTypes
from new_couchbase.durability import DurabilityLevel
from new_couchbase.classic.management.options import ValidBucketMgmtOptions

from couchbase.pycbc_core import (bucket_mgmt_operations,
                                  management_operation,
                                  mgmt_operations)

if TYPE_CHECKING:
    from new_couchbase.classic.cluster import Cluster
    from new_couchbase.common.management.buckets import BucketSettings, CreateBucketSettings
    from new_couchbase.management.options import (CreateBucketOptions,
                                                  DropBucketOptions,
                                                  FlushBucketOptions,
                                                  GetAllBucketOptions,
                                                  GetBucketOptions,
                                                  UpdateBucketOptions)

class BucketManagerCore:

    _ERROR_MAPPING = {'Bucket with given name (already|still) exists': BucketAlreadyExistsException,
                      'Requested resource not found': BucketDoesNotExistException,
                      r'.*Flush is disabled for the bucket.*': BucketNotFlushableException,
                    #   r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException,
                      r'.*is supported only with developer preview enabled.*': FeatureUnavailableException}

    def __init__(self,
                 cluster # type: Cluster
                 ):
        self._connection = cluster.connection

    def create_bucket(self,
                      settings,  # type: CreateBucketSettings
                      *options,  # type: CreateBucketOptions
                      **kwargs   # type: Dict[str, Any]
                      ) -> None:

        mgmt_kwargs = {
            'conn': self._connection,
            'mgmt_op': mgmt_operations.BUCKET.value,
            'op_type': bucket_mgmt_operations.CREATE_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        params = BucketManagerCore.bucket_settings_to_server(settings)

        mgmt_kwargs['op_args'] = {
            'bucket_settings': params
        }

        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.Create)
        final_args = parse_options(valid_opts, kwargs, *options)
        if final_args.get('timeout', None) is not None:
            mgmt_kwargs['timeout'] = final_args.get('timeout')

        return management_operation(**mgmt_kwargs)
    

    def drop_bucket(self,
                    bucket_name,  # type: str
                    *options,     # type: DropBucketOptions
                    **kwargs      # type: Any
                    ) -> None:

        if is_null_or_empty(bucket_name):
            raise InvalidArgumentException("Bucket name cannot be None or empty.")

        if not isinstance(bucket_name, str):
            raise InvalidArgumentException("Bucket name must be a str.")

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.DROP_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        mgmt_kwargs["op_args"] = {
            "bucket_name": bucket_name
        }

        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.Drop)
        final_args = parse_options(valid_opts, kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)
    
    def flush_bucket(self,
                     bucket_name,   # type: str
                     *options,      # type: FlushBucketOptions
                     **kwargs       # type: Any
                     ) -> None:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.FLUSH_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        mgmt_kwargs["op_args"] = {
            "bucket_name": bucket_name
        }

        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.Flush)
        final_args = parse_options(valid_opts, kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_all_buckets(self,
                        *options,  # type: GetAllBucketOptions
                        **kwargs  # type: Any
                        ) -> List[Any]:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.GET_ALL_BUCKETS.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.GetAll)
        final_args = parse_options(valid_opts, kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_bucket(self,
                   bucket_name,   # type: str
                   *options,      # type: GetBucketOptions
                   **kwargs       # type: Any
                   ) -> Any:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.GET_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        mgmt_kwargs["op_args"] = {
            "bucket_name": bucket_name
        }

        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.Get)
        final_args = parse_options(valid_opts, kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def update_bucket(self,
                      settings,  # type: BucketSettings
                      *options,  # type: UpdateBucketOptions
                      **kwargs  # type: Any
                      ) -> None:

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.BUCKET.value,
            "op_type": bucket_mgmt_operations.UPDATE_BUCKET.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        params = settings.transform_to_dest()

        mgmt_kwargs["op_args"] = {
            "bucket_settings": params
        }

        valid_opts = ValidBucketMgmtOptions.get_valid_options(BucketMgmtOptionTypes.Update)
        final_args = parse_options(valid_opts, kwargs, *options)
        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)


    @staticmethod
    def bucket_settings_to_server(settings, # type: Dict[str, Any]
                                  ) -> Dict[str, Any]:
        """**INTERNAL**"""
        output = dict()
        bucket_type = settings.get('bucket_type', BucketType.COUCHBASE)
        output['bucketType'] = enum_to_str(bucket_type, BucketType)
        compression_mode = settings.get('compression_mode', None)
        if compression_mode:
            output['compressionMode'] = enum_to_str(compression_mode, CompressionMode)
        conflict_resolution_type = settings.get('conflict_resolution_type', None)
        if conflict_resolution_type:
            output['conflictResolutionType'] = enum_to_str(conflict_resolution_type, ConflictResolutionType)
        eviction_policy = settings.get('eviction_policy', None)
        if eviction_policy:
            output['evictionPolicy'] = enum_to_str(eviction_policy, EvictionPolicyType)
        output['flushEnabled'] = settings.get('flush_enabled', False)
        max_expiry = settings.get('max_expiry', None)
        if max_expiry:
            output['maxExpiry'] = to_seconds(max_expiry)
        max_ttl = settings.get('max_ttl', None)
        if max_ttl:
            output['maxTTL'] = validate_int(max_ttl)
        minimum_durability_level = settings.get('minimum_durability_level', None)
        if minimum_durability_level:
            output['durabilityMinLevel'] = enum_to_str(minimum_durability_level,
                                                       DurabilityLevel,
                                                       DurabilityLevel.to_server_str)
        name = settings.get('name', None)
        if name:
            output['name'] = validate_str(name)
        num_replicas = settings.get('num_replicas', 0)
        output['numReplicas'] = validate_int(num_replicas)
        ram_quota_mb = settings.get('ram_quota_mb', None)
        if ram_quota_mb:
            output['ramQuotaMB'] = validate_int(ram_quota_mb)
        replica_index = settings.get('replica_index', None)
        if replica_index:
            output['replicaIndex'] = validate_bool(replica_index)
        storage_backend = settings.get('storage_backend', StorageBackend.COUCHSTORE)
        output['storageBackend'] = enum_to_str(storage_backend, StorageBackend)
        return output
    
    @staticmethod
    def bucket_settings_from_server(settings, # type: Dict[str, Any]
                                    ) -> Dict[str, Any]:
        """**INTERNAL**"""
        output = dict()
        bucket_type = settings.get('bucketType', None)
        if bucket_type:
            output['bucket_type'] = str_to_enum(bucket_type, BucketType)
        compression_mode = settings.get('compressionMode', None)
        if compression_mode:
            output['compression_mode'] = str_to_enum(compression_mode, CompressionMode)
        conflict_resolution_type = settings.get('conflictResolutionType', None)
        if conflict_resolution_type:
            output['conflict_resolution_type'] = str_to_enum(conflict_resolution_type, ConflictResolutionType)
        eviction_policy = settings.get('evictionPolicy', None)
        if eviction_policy:
            output['eviction_policy'] = str_to_enum(eviction_policy, EvictionPolicyType)
        output['flush_enabled'] = settings.get('flushEnabled', None)
        output['max_expiry'] = settings.get('maxExpiry', None)
        output['max_ttl'] = settings.get('maxTTL', None)

        minimum_durability_level = settings.get('durabilityMinLevel', None)
        output['minimum_durability_level'] = str_to_enum(minimum_durability_level,
                                                        DurabilityLevel,
                                                        DurabilityLevel.from_server_str)
        output['name'] = settings.get('name', None)
        output['num_replicas'] = settings.get('numReplicas', None)
        output['ram_quota_mb'] = settings.get('ramQuotaMB', None)
        output['replica_index'] = settings.get('replicaIndex', None)
        storage_backend = settings.get('storageBackend', StorageBackend.COUCHSTORE)
        if storage_backend:
            output['storage_backend'] = str_to_enum(storage_backend, StorageBackend)
        return output