from typing import Any, Dict, List, Optional, TYPE_CHECKING

from couchbase.management.buckets import CreateBucketSettings, BucketType, EvictionPolicyType, CompressionMode, ConflictResolutionType, StorageBackend, BucketSettings

from couchbase.exceptions import InvalidArgumentException

from protostellar import bucket_admin_grpc_module as buckets
from protostellar.proto.couchbase.admin.bucket import v1_pb2

from protostellar.options import parse_options
from protostellar.management.options import (CreateBucketOptions,
                                        #   DropBucketOptions,
                                        #   FlushBucketOptions,
                                          GetAllBucketOptions,
                                          GetBucketOptions,
                                        #   UpdateBucketOptions
                                        )

# if TYPE_CHECKING:
#     from couchbase.management.buckets import BucketType, EvictionPolicyType, CompressionMode, ConflictResolutionType, StorageBackend


class BucketManager:

    def __init__(self, cluster):
        self._channel = cluster.channel
        self._buckets = buckets.BucketAdminStub(self._channel)

    # TODO:  I don't think these methods are necessary, probably can work off Enum values

    @staticmethod
    def to_protostellar_bucket_type(bucket_type # type: BucketType
        ) -> v1_pb2.BucketType:
        if bucket_type == BucketType.COUCHBASE:
            return v1_pb2.BucketType.COUCHBASE
        elif bucket_type == BucketType.MEMCACHED:
            return v1_pb2.BucketType.MEMCACHED
        elif bucket_type == BucketType.EPHEMERAL:
            return v1_pb2.BucketType.EPHEMERAL
        else:
            raise InvalidArgumentException(f'Invalid bucket_type: {bucket_type}')

    @staticmethod
    def to_protostellar_eviction_mode(eviction_policy # type: EvictionPolicyType
        ) -> v1_pb2.EvictionMode:
        if eviction_policy == EvictionPolicyType.NOT_RECENTLY_USED:
            return v1_pb2.EvictionMode.NOT_RECENTLY_USED
        elif eviction_policy == EvictionPolicyType.NO_EVICTION:
            return v1_pb2.EvictionMode.NONE
        elif eviction_policy == EvictionPolicyType.FULL:
            return v1_pb2.EvictionMode.FULL
        elif eviction_policy == EvictionPolicyType.VALUE_ONLY:
            return v1_pb2.EvictionMode.VALUE_ONLY
        else:
            raise InvalidArgumentException(f'Invalid eviction_policy: {eviction_policy}')

    @staticmethod
    def to_protostellar_compression_mode(compression_mode # type: CompressionMode
        ) -> v1_pb2.CompressionMode:
        if compression_mode == CompressionMode.OFF:
            return v1_pb2.CompressionMode.OFF
        elif compression_mode == CompressionMode.PASSIVE:
            return v1_pb2.CompressionMode.PASSIVE
        elif compression_mode == CompressionMode.ACTIVE:
            return v1_pb2.CompressionMode.ACTIVE
        else:
            raise InvalidArgumentException(f'Invalid compression_mode: {compression_mode}')

    @staticmethod
    def to_protostellar_conflict_resolution_type(conflict_resolution_type # type: ConflictResolutionType
        ) -> v1_pb2.ConflictResolutionType:
        if conflict_resolution_type == ConflictResolutionType.TIMESTAMP:
            return v1_pb2.ConflictResolutionType.TIMESTAMP
        elif conflict_resolution_type == ConflictResolutionType.SEQUENCE_NUMBER:
            return v1_pb2.ConflictResolutionType.TIMESTAMP
        elif conflict_resolution_type == ConflictResolutionType.CUSTOM:
            return v1_pb2.ConflictResolutionType.CUSTOM
        else:
            raise InvalidArgumentException(f'Invalid conflict_resolution_type: {conflict_resolution_type}')

    @staticmethod
    def to_protostellar_storage_backend(storage_backend # type: StorageBackend
        ) -> v1_pb2.StorageBackend:
        if storage_backend == StorageBackend.COUCHSTORE:
            return v1_pb2.StorageBackend.COUCHSTORE
        elif storage_backend == StorageBackend.MAGMA:
            return v1_pb2.StorageBackend.MAGMA
        else:
            raise InvalidArgumentException(f'Invalid storage_backend: {storage_backend}')

    def create_bucket(self,
                      settings,  # type: CreateBucketSettings
                      options=None,  # type: Optional[CreateBucketOptions]
                      **kwargs   # type: Dict[str, Any]
                      ) -> None:
        """Creates a new bucket.

        Args:
            settings (:class:`couchbase.management.buckets.CreateBucketSettings`): The settings to use for the new bucket.
            options (:class:`~protostellar.management.options.CreateBucketOptions`): Optional parameters for this
                operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used as optional parameters
                for this operation.

        Raises:
            :class:`~couchbase.exceptions.BucketAlreadyExistsException`: If the bucket already exists.
            :class:`~couchbase.exceptions.InvalidArgumentsException`: If an invalid type or value is provided for the
                settings argument.
        """
        final_opts = parse_options(options, **kwargs)
        # TODO:  add final_opts

        req_args = {
            'bucket_name': settings.name,
            'bucket_type': BucketManager.to_protostellar_bucket_type(settings.bucket_type),
            # TODO:  what is the default?
            'ram_quota_bytes': settings.get('ram_quota_mb', 100),
        }

        num_replicas = settings.get('num_replicas', 0)
        if num_replicas is not None:
            req_args['num_replicas'] = num_replicas

        flush_enabled = settings.get('flush_enabled', None)
        if flush_enabled:
            req_args['flush_enabled'] = flush_enabled

        replica_indexes = settings.get('replica_index', None)
        if replica_indexes:
            req_args['replica_indexes'] = replica_indexes

        eviction_mode = settings.get('eviction_policy', None)
        if eviction_mode:
            req_args['eviction_mode'] = BucketManager.to_protostellar_eviction_mode(eviction_mode)

        max_expiry_secs = settings.get('max_expiry', None)
        if max_expiry_secs:
            req_args['max_expiry_secs'] = max_expiry_secs.total_seconds()

        compression_mode = settings.get('compression_mode', None)
        if compression_mode:
            req_args['compression_mode'] = BucketManager.to_protostellar_compression_mode(compression_mode)

        minimum_durability_level = settings.get('minimum_durability_level', None)
        if minimum_durability_level:
            req_args['minimum_durability_level'] = minimum_durability_level.value

        storage_backend = settings.get('storage_backend', None)
        if storage_backend:
            req_args['storage_backend'] = BucketManager.to_protostellar_storage_backend(storage_backend)

        conflict_resolution_type = settings.get('conflict_resolution_type', None)
        if conflict_resolution_type:
            req_args['conflict_resolution_type'] = BucketManager.to_protostellar_conflict_resolution_type(conflict_resolution_type)

        req = v1_pb2.CreateBucketRequest(**req_args)
        self._buckets.CreateBucket(req)

    def get_bucket(self,
                   bucket_name,   # type: str
                   options=None,      # type: Optional[GetBucketOptions]
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
        buckets = self.get_all_buckets()

    def get_all_buckets(self,
                        options=None,  # type: Optional[GetAllBucketOptions]
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
        final_opts = parse_options(options, **kwargs)
        # TODO:  add final_opts
        req = v1_pb2.ListBucketsRequest()
        res = self._buckets.ListBuckets(req)
        print(res)
        