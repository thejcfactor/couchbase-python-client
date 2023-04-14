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
                    Union,
                    overload)

from new_couchbase.exceptions import InvalidArgumentException
from new_couchbase.common._utils import is_null_or_empty

if TYPE_CHECKING:
    from new_couchbase.durability import DurabilityLevel

class EvictionPolicyType(Enum):
    NOT_RECENTLY_USED = "nruEviction"
    NO_EVICTION = "noEviction"
    FULL = "fullEviction"
    VALUE_ONLY = "valueOnly"


class EjectionMethod(Enum):
    FULL_EVICTION = "fullEviction"
    VALUE_ONLY = "valueOnly"


class BucketType(Enum):
    COUCHBASE = "membase"
    MEMCACHED = "memcached"
    EPHEMERAL = "ephemeral"


class CompressionMode(Enum):
    OFF = "off"
    PASSIVE = "passive"
    ACTIVE = "active"


class ConflictResolutionType(Enum):
    """
    Specifies the conflict resolution type to use for the bucket.

    Members:
    TIMESTAMP: specifies to use timestamp conflict resolution
    SEQUENCE_NUMBER: specifies to use sequence number conflict resolution
    CUSTOM: **VOLATILE** This API is subject to change at any time. In Couchbase Server 7.1,
    this feature is only available in "developer-preview" mode. See the UI XDCR settings.

    """
    TIMESTAMP = "lww"
    SEQUENCE_NUMBER = "seqno"
    CUSTOM = "custom"


class StorageBackend(Enum):
    """
    **UNCOMMITTED**
    StorageBackend is part of an uncommitted API that is unlikely to change,
    but may still change as final consensus on its behavior has not yet been reached.

    Specifies the storage type to use for the bucket.
    """
    UNDEFINED = "undefined"
    COUCHSTORE = "couchstore"
    MAGMA = "magma"

class BucketSettings(dict):
    @overload
    def __init__(self,
                 bucket_type=None,  # type: Optional[BucketType]
                 compression_mode=None,  # type: Optional[CompressionMode]
                 eviction_policy=None,  # type: Optional[EvictionPolicyType]
                 flush_enabled=False,  # type: Optional[bool]
                 max_expiry=None,  # type: Optional[Union[timedelta,float,int]]
                 max_ttl=None,  # type: Optional[Union[timedelta,float,int]]
                 minimum_durability_level=None,  # type: Optional[DurabilityLevel]
                 name=None,  # type: str
                 num_replicas=None,  # type: Optional[int]
                 ram_quota_mb=None,  # type: int
                 replica_index=None,  # type: Optional[bool]
                 storage_backend=None,  # type: Optional[StorageBackend]
                 ):
        pass

    def __init__(self, **kwargs):
        """BucketSettings provides a means of mapping bucket settings into an object.

        """
        bucket_name = kwargs.get('name', None)
        if is_null_or_empty(bucket_name):
            raise InvalidArgumentException('Must provide a non-empty bucket name.')
        if kwargs.get('bucket_type', None) == 'couchbase':
            kwargs['bucket_type'] = BucketType.COUCHBASE
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def bucket_type(self) -> BucketType:
        """BucketType {couchbase (sent on wire as membase), memcached, ephemeral}
        The type of the bucket. Default to couchbase."""
        return self.get('bucket_type', None)
    
    @property
    def compression_mode(self) -> Optional[CompressionMode]:
        """{off | passive | active} - The compression mode to use."""
        return self.get('compression_mode', None)
    
    @property
    def eviction_policy(self) -> Optional[EvictionPolicyType]:
        """{fullEviction | valueOnly}. The eviction policy to use."""
        return self.get('eviction_policy', None)

    @property
    def flush_enabled(self) -> bool:
        """True if flush enabled on bucket, False otherwise"""
        return self.get('flush_enabled', False)

    @property
    def max_expiry(self) -> timedelta:
        """
           Value for the max expiry of new documents created without an expiry.
        """
        return self.get('max_expiry', None)

    @property
    def max_ttl(self) -> Optional[int]:
        """
         **DEPRECATED** use max_expiry
            Value for the maxTTL of new documents created without a ttl.
        """
        return self.get('max_ttl', None)

    @property
    def minimum_durability_level(self) -> Optional[DurabilityLevel]:
        """The durability level to use for the bucket."""
        return self.get('minimum_durability_level', None)

    @property
    def name(self) -> str:
        """ Bucket name"""
        return self.get('name')

    @property
    def num_replicas(self) -> int:
        """NumReplicas (int) - The number of replicas for documents."""
        return self.get('replica_number')

    @property
    def ram_quota_mb(self) -> int:
        """ram_quota_mb (int) - The RAM quota for the bucket."""
        return self.get('ram_quota_mb')

    @property
    def replica_index(self) -> bool:
        """ Whether replica indexes should be enabled for the bucket."""
        return self.get('replica_index')

    @property
    def storage_backend(self) -> Union[StorageBackend, None]:
        """
        {couchstore | magma | undefined} - The storage backend to use.
        """
        return self.get('storage_backend', None)

class CreateBucketSettings(BucketSettings):
    @overload
    def __init__(self,
                 name=None,  # type: str
                 flush_enabled=False,  # type: bool
                 ram_quota_mb=None,  # type: int
                 num_replicas=0,  # type: int
                 replica_index=None,  # type: bool
                 bucket_type=None,  # type: BucketType
                 eviction_policy=None,  # type: EvictionPolicyType
                 max_ttl=None,  # type: Union[timedelta,float,int]
                 max_expiry=None,  # type: Union[timedelta,float,int]
                 compression_mode=None,  # type: CompressionMode
                 conflict_resolution_type=None,  # type: ConflictResolutionType
                 bucket_password='',  # type: str
                 ejection_method=None,  # type: EjectionMethod
                 storage_backend=None  # type: StorageBackend
                 ):
        pass
        
    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)