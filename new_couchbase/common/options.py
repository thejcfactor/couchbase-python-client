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

from abc import ABC, abstractmethod
import copy
import ctypes
from datetime import timedelta
from enum import Enum, IntEnum
from typing import Any, Dict, Iterable, List, Optional, Union, overload, TYPE_CHECKING



from new_couchbase.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from new_couchbase.api.authentication import Authenticator
    from new_couchbase.serializer import Serializer
    from new_couchbase.transcoder import Transcoder
    from new_couchbase.common._utils import JSONType
    from new_couchbase.diagnostics import ClusterState, ServiceType
    from new_couchbase.durability import DurabilityType
    from new_couchbase.mutation_state import MutationState
    from new_couchbase.subdocument import StoreSemantics
    from new_couchbase.n1ql import QueryProfile, QueryScanConsistency

def parse_options(
    valid_opts,  # type: Dict[str,Any]
    arg_vars,  # type: Optional[Dict[str,Any]]
    *options  # type: OptionsBase
) -> Dict[str, Any]:

    arg_vars = copy.copy(arg_vars) if arg_vars else {}
    temp_options = (
        copy.copy(
            options[0]) if (
            options and options[0]) else dict())
    kwargs = arg_vars.pop("kwargs", {})
    temp_options.update(kwargs)
    temp_options.update(arg_vars)

    final_options = {}
    for opt_key, opt_value in temp_options.items():
        if opt_key not in valid_opts:
            continue
        for final_key, transform in valid_opts[opt_key].items():
            converted = transform(opt_value)
            if converted is not None:
                final_options[final_key] = converted

    return final_options

"""

Couchbase Python SDK Options related Enumerations

"""


class LockMode(IntEnum):
    WAIT = 0
    EXC = 1
    NONE = 2


class TLSVerifyMode(Enum):
    NONE = 'none'
    PEER = 'peer'
    NO_VERIFY = 'no_verify'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.NONE.value:
                return cls.NONE
            elif value == cls.PEER.value:
                return cls.PEER
            elif value == cls.NO_VERIFY.value:
                return cls.NONE

        raise InvalidArgumentException(message=(f"{value} is not a valid TLSVerifyMode option. "
                                                "Excepted str representation of type TLSVerifyMode."))

    @classmethod
    def to_str(cls, value  # type: Union[TLSVerifyMode, str]
               ) -> str:
        if isinstance(value, TLSVerifyMode):
            if value == cls.NO_VERIFY:
                return cls.NONE.value
            return value.value
        if isinstance(value, str):
            if value == cls.NONE.value:
                return cls.NONE.value
            elif value == cls.PEER.value:
                return cls.PEER.value
            elif value == cls.NO_VERIFY.value:
                return cls.NONE.value

        raise InvalidArgumentException(message=(f"{value} is not a valid TLSVerifyMode option. "
                                                "Excepted TLS verify mode to be either of type "
                                                "TLSVerifyMode or str representation "
                                                "of TLSVerifyMode."))


class IpProtocol(Enum):
    Any = 'any'
    ForceIPv4 = 'force_ipv4'
    ForceIPv6 = 'force_ipv6'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.Any.value:
                return cls.Any
            elif value == cls.ForceIPv4.value:
                return cls.ForceIPv4
            elif value == cls.ForceIPv6.value:
                return cls.ForceIPv6

        raise InvalidArgumentException(message=(f"{value} is not a valid IpProtocol option. "
                                                "Excepted str representation of type IpProtocol."))

    @classmethod
    def to_str(cls, value  # type: Union[IpProtocol, str]
               ) -> str:
        if isinstance(value, IpProtocol):
            return value.value
        if isinstance(value, str):
            if value == cls.Any.value:
                return cls.Any.value
            elif value == cls.ForceIPv4.value:
                return cls.ForceIPv4.value
            elif value == cls.ForceIPv6.value:
                return cls.ForceIPv6.value

        raise InvalidArgumentException(message=(f"{value} is not a valid IpProtocol option. "
                                                "Excepted IP Protocol mode to be either of type "
                                                "IpProtocol or str representation "
                                                "of IpProtocol."))


class Compression(Enum):
    """
    Can be one of:
        NONE:
            The client will not compress or decompress the data.
        IN:
            The data coming back from the server will be decompressed, if it was compressed.
        OUT:
            The data coming into server will be compressed.
        INOUT:
            The data will be compressed on way in, decompressed on way out of server.
        FORCE:
            By default the library will send a HELLO command to the server to determine whether compression
            is supported or not.  Because commands may be
            pipelined prior to the scheduing of the HELLO command it is possible that the first few commands
            may not be compressed when schedule due to the library not yet having negotiated settings with the
            server. Setting this flag will force the client to assume that all servers support compression
            despite a HELLO not having been intially negotiated.
    """

    @classmethod
    def from_int(cls, val):
        if val == 0:
            return cls.NONE
        elif val == 1:
            return cls.IN
        elif val == 2:
            return cls.OUT
        elif val == 3:
            return cls.INOUT
        elif val == 7:
            # note that the lcb flag is a 4, but when you set "force" in the connection
            # string, it sets it as INOUT|FORCE.
            return cls.FORCE
        else:
            raise InvalidArgumentException(
                "cannot convert {} to a Compression".format(val)
            )

    NONE = "off"
    IN = "inflate_only"
    OUT = "deflate_only"
    INOUT = "on"
    FORCE = "force"


class KnownConfigProfiles(Enum):
    """
    **VOLATILE** This API is subject to change at any time.

    Represents the name of a specific configuration profile that is associated with predetermined cluster options.

    """
    WanDevelopment = 'wan_development'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.WanDevelopment.value:
                return cls.WanDevelopment

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Excepted str representation of type KnownConfigProfiles."))

    @classmethod
    def to_str(cls, value  # type: Union[KnownConfigProfiles, str]
               ) -> str:
        if isinstance(value, KnownConfigProfiles):
            return value.value

        # just retun the str to allow for future customer config profiles
        if isinstance(value, str):
            return value

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Excepted config profile to be either of type "
                                                "KnownConfigProfiles or str representation "
                                                "of KnownConfigProfiles."))


class OptionTypes(Enum):
    Analytics = 'AnalyticsOptions'
    Append = 'AppendOptions'
    Cluster = 'ClusterOptions'
    ClusterTimeout = 'ClusterTimeoutOptions'
    ClusterTracing = 'ClusterTracingOptions'
    Decrement = 'DecrementOptions'
    Diagnostics = 'DiagnosticsOptions'
    Exists = 'ExistsOptions'
    Get = 'GetOptions'
    GetAllReplicas = 'GetAllReplicasOptions'
    GetAndLock = 'GetAndLockOptions'
    GetAndTouch = 'GetAndTouchOptions'
    GetAnyReplica = 'GetAnyReplicaOptions'
    Increment = 'IncrementOptions'
    Insert = 'InsertOptions'
    LookupIn = 'LookupInOptions'
    MutateIn = 'MutateInOptions'
    Ping = 'PingOptions'
    Prepend = 'PrependOptions'
    Query = 'QueryOptions'
    Remove = 'RemoveOptions'
    Replace = 'ReplaceOptions'
    Search = 'SearchOptions'
    Touch = 'TouchOptions'
    Unlock = 'UnlockOptions'
    Upsert = 'UpsertOptions'
    View = 'ViewOptions'
    WaitUntilReady = 'WaitUntilReadyOptions'
    
"""

Python SDK Cluster Options Base Classes

"""

class ClusterTimeoutOptionsBase(dict):

    @overload
    def __init__(
        self,
        bootstrap_timeout=None,  # type: Optional[timedelta]
        resolve_timeout=None,  # type: Optional[timedelta]
        connect_timeout=None,  # type: Optional[timedelta]
        kv_timeout=None,  # type: Optional[timedelta]
        kv_durable_timeout=None,  # type: Optional[timedelta]
        views_timeout=None,  # type: Optional[timedelta]
        query_timeout=None,  # type: Optional[timedelta]
        analytics_timeout=None,  # type: Optional[timedelta]
        search_timeout=None,  # type: Optional[timedelta]
        management_timeout=None,  # type: Optional[timedelta]
        dns_srv_timeout=None,  # type: Optional[timedelta]
        idle_http_connection_timeout=None,  # type: Optional[timedelta]
        config_idle_redial_timeout=None,  # type: Optional[timedelta]
        config_total_timeout=None  # type: Optional[timedelta]
    ):
        """ClusterTimeoutOptions instance."""

    def __init__(self, **kwargs):
        # kv_timeout = kwargs.pop('kv_timeout', None)
        # if kv_timeout:
        #     kwargs["key_value_timeout"] = kv_timeout
        # kv_durable_timeout = kwargs.pop('kv_durable_timeout', None)
        # if kv_durable_timeout:
        #     kwargs["key_value_durable_timeout"] = kv_durable_timeout

        # legacy...
        kwargs.pop('config_total_timeout', None)

        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        super().__init__(**kwargs)

    def as_dict(self):
        opts = {}
        allowed_opts = ClusterTimeoutOptionsBase.get_allowed_option_keys()
        for k, v in self.items():
            if k not in allowed_opts:
                continue
            if v is None:
                continue
            if isinstance(v, timedelta):
                opts[k] = v.total_seconds()
            elif isinstance(v, (int, float)):
                opts[k] = v
        return opts

    @staticmethod
    def get_allowed_option_keys(use_transform_keys=False  # type: Optional[bool]
                                ) -> List[str]:
        if use_transform_keys is True:
            keys = []
            for val in ClusterTimeoutOptionsBase._VALID_OPTS.values():
                keys.append(list(val.keys())[0])
            return keys

        return list(ClusterTimeoutOptionsBase._VALID_OPTS.keys())


class ClusterTracingOptionsBase(dict):

    @overload
    def __init__(
        self,
        tracing_threshold_kv=None,  # type: Optional[timedelta]
        tracing_threshold_view=None,  # type: Optional[timedelta]
        tracing_threshold_query=None,  # type: Optional[timedelta]
        tracing_threshold_search=None,  # type: Optional[timedelta]
        tracing_threshold_analytics=None,  # type: Optional[timedelta]
        tracing_threshold_eventing=None,  # type: Optional[timedelta]
        tracing_threshold_management=None,  # type: Optional[timedelta]
        tracing_threshold_queue_size=None,  # type: Optional[int]
        tracing_threshold_queue_flush_interval=None,  # type: Optional[timedelta]
        tracing_orphaned_queue_size=None,  # type: Optional[int]
        tracing_orphaned_queue_flush_interval=None,  # type: Optional[timedelta]
    ):
        """ClusterTracingOptions instance."""

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    def as_dict(self):
        opts = {}
        allowed_opts = ClusterTracingOptionsBase.get_allowed_option_keys()
        for k, v in self.items():
            if k not in allowed_opts:
                continue
            if v is None:
                continue
            if isinstance(v, timedelta):
                opts[k] = v.total_seconds()
            elif isinstance(v, (int, float)):
                opts[k] = v
        return opts

    @staticmethod
    def get_allowed_option_keys(use_transform_keys=False  # type: Optional[bool]
                                ) -> List[str]:
        if use_transform_keys is True:
            keys = []
            for val in ClusterTracingOptionsBase._VALID_OPTS.values():
                keys.append(list(val.keys())[0])
            return keys

        return list(ClusterTracingOptionsBase._VALID_OPTS.keys())


class ClusterOptionsBase(dict):

    @overload
    def __init__(
        self,
        authenticator,  # type: Authenticator
        timeout_options=None,  # type: Optional[ClusterTimeoutOptionsBase]
        tracing_options=None,  # type: Optional[ClusterTracingOptionsBase]
        enable_tls=None,    # type: Optional[bool]
        enable_mutation_tokens=None,    # type: Optional[bool]
        enable_tcp_keep_alive=None,    # type: Optional[bool]
        ip_protocol=None,    # type: Optional[Union[IpProtocol, str]]
        enable_dns_srv=None,    # type: Optional[bool]
        show_queries=None,    # type: Optional[bool]
        enable_unordered_execution=None,    # type: Optional[bool]
        enable_clustermap_notification=None,    # type: Optional[bool]
        enable_compression=None,    # type: Optional[bool]
        enable_tracing=None,    # type: Optional[bool]
        enable_metrics=None,    # type: Optional[bool]
        network=None,    # type: Optional[str]
        tls_verify=None,    # type: Optional[Union[TLSVerifyMode, str]]
        serializer=None,  # type: Optional[Serializer]
        transcoder=None,  # type: Optional[Transcoder]
        tcp_keep_alive_interval=None,  # type: Optional[timedelta]
        config_poll_interval=None,  # type: Optional[timedelta]
        config_poll_floor=None,  # type: Optional[timedelta]
        max_http_connections=None,  # type: Optional[int]
        user_agent_extra=None,  # type: Optional[str]
        logging_meter_emit_interval=None,  # type: Optional[timedelta]
        # TODO: Change Any type
        transaction_config=None,  # type: Optional[Any]
        log_redaction=None,  # type: Optional[bool]
        compression=None,  # type: Optional[Compression]
        compression_min_size=None,  # type: Optional[int]
        compression_min_ratio=None,  # type: Optional[float]
        lockmode=None,  # type: Optional[LockMode]
        # TODO: Change Any type
        tracer=None,  # type: Optional[Any]
        # TODO: Change Any type
        meter=None,  # type: Optional[Any]
        dns_nameserver=None,  # type: Optional[str]
        dns_port=None,  # type: Optional[int]
    ):
        """ClusterOptions instance."""

    def __init__(self,
                 authenticator,  # type: Authenticator
                 **kwargs
                 ):

        if authenticator:
            kwargs["authenticator"] = authenticator

        # flatten tracing and timeout options
        tracing_opts = kwargs.pop('tracing_options', {})
        if tracing_opts:
            for k, v in tracing_opts.items():
                if k not in kwargs:
                    kwargs[k] = v

        timeout_opts = kwargs.pop('timeout_options', {})
        if timeout_opts:
            for k, v in timeout_opts.items():
                if k not in kwargs:
                    kwargs[k] = v

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @staticmethod
    def get_allowed_option_keys(cluster_opts_only=False,  # type: Optional[bool]
                                use_transform_keys=False  # type: Optional[bool]
                                ) -> List[str]:
        if use_transform_keys is True:
            keys = []
            for val in ClusterOptionsBase._VALID_OPTS.values():
                keys.append(list(val.keys())[0])

            if cluster_opts_only is True:
                return keys

            keys.extend(ClusterTimeoutOptionsBase.get_allowed_option_keys(use_transform_keys=True))
            keys.extend(ClusterTracingOptionsBase.get_allowed_option_keys(use_transform_keys=True))

            return keys

        if cluster_opts_only is True:
            return list(ClusterOptionsBase._VALID_OPTS.keys())

        valid_keys = ClusterTimeoutOptionsBase.get_allowed_option_keys()
        valid_keys.extend(ClusterTracingOptionsBase.get_allowed_option_keys())
        valid_keys.extend(list(ClusterOptionsBase._VALID_OPTS.keys()))

        return valid_keys

    @staticmethod
    def get_valid_options() -> Dict[str, Any]:
        valid_opts = copy.copy(ClusterTimeoutOptionsBase._VALID_OPTS)
        valid_opts.update(copy.copy(ClusterTracingOptionsBase._VALID_OPTS))
        valid_opts.update(copy.copy(ClusterOptionsBase._VALID_OPTS))
        return valid_opts

class ConfigProfile(ABC):
    """
    **VOLATILE** This API is subject to change at any time.

    This is an abstract base class intended to use with creating Configuration Profiles.  Any derived class
    will need to implement the :meth:`apply` method.
    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def apply(self,
              options  # type: ClusterOptionsBase
              ) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided options to ClusterOptions. This method will need to be implemented in derived classes.

        Args:
            options (:class:`~couchbase.options.ClusterOptions`): The options the profile will apply toward.
        """
        pass


"""

Python SDK Base Options Classes

"""

class OptionsBase(dict):
    @overload
    def __init__(self,
                 timeout=None,       # type: Optional[timedelta]
                 span=None,  # type: Optional[Any]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    def timeout(self,
                timeout,  # type: timedelta
                ) -> OptionsBase:
        self['timeout'] = timeout
        return self

    def span(self,
             span,  # type: Any
             ) -> OptionsBase:
        self['span'] = span
        return self

class OptionsDurabilityBase(dict):
    @overload
    def __init__(self,
                 durability=None, # type: Optional[Any]
                 span=None,  # type: Optional[Any]
                 timeout=None,       # type: Optional[timedelta]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    def durability(self,
             durability,  # type: Any
             ) -> OptionsBase:
        self['durability'] = durability
        return self

    def span(self,
             span,  # type: Any
             ) -> OptionsBase:
        self['span'] = span
        return self

    def timeout(self,
                timeout,  # type: timedelta
                ) -> OptionsBase:
        self['timeout'] = timeout
        return self

"""

Python SDK Diagnostic Operation Options Base Classes

"""

class DiagnosticsOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 report_id=None, # type: Optional[str]
                 timeout=None,       # type: Optional[timedelta]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class PingOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 report_id=None, # type: Optional[str]
                 service_types=None,  # type: List[Any]
                 timeout=None,       # type: Optional[timedelta]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)    



class WaitUntilReadyOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 desired_state=None,     # type: ClusterState
                 service_types=None  # type: Iterable[ServiceType]
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

"""

Python SDK Key-Value Operation Options Base Classes

"""


# Binary Operations

class IncrementOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,      # type: Optional[timedelta]
                 expiry=None,       # type: Optional[timedelta]
                 durability=None,   # type: Optional[DurabilityType]
                 delta=None,         # type: Optional[DeltaValueBase]
                 initial=None,      # type: Optional[SignedInt64Base]
                 span=None         # type: Optional[Any]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DecrementOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,      # type: Optional[timedelta]
                 expiry=None,       # type: Optional[timedelta]
                 durability=None,   # type: Optional[DurabilityType]
                 delta=None,         # type: Optional[DeltaValueBase]
                 initial=None,      # type: Optional[SignedInt64Base]
                 span=None         # type: Optional[Any]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class AppendOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,      # type: Optional[timedelta]
                 durability=None,   # type: Optional[DurabilityType]
                 cas=None,          # type: Optional[int]
                 span=None         # type: Optional[Any]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class PrependOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,      # type: Optional[timedelta]
                 durability=None,   # type: Optional[DurabilityType]
                 cas=None,          # type: Optional[int]
                 span=None         # type: Optional[Any]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

# Multi Operations

# Standard Operations

class ExistsOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 timeout=None  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class GetOptionsBase(OptionsBase):
    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        with_expiry=None,  # type: Optional[bool]
        project=None,  # type: Optional[Iterable[str]]
        transcoder=None  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def with_expiry(self) -> bool:
        return self.get("with_expiry", False)

    @property
    def project(self) -> Iterable[str]:
        return self.get("project", [])

class GetAllReplicasOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class GetAndLockOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class GetAndTouchOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class GetAnyReplicaOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class InsertOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 expiry=None,  # type: Optional[timedelta]
                 durability=None,  # type: Optional[DurabilityType]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class RemoveOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 cas=None,  # type: Optional[int]
                 durability=None  # type: Optional[DurabilityType]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class ReplaceOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 expiry=None,  # type: Optional[timedelta]
                 cas=None,  # type: Optional[int]
                 preserve_expiry=False,  # type: Optional[bool]
                 durability=None,  # type: Optional[DurabilityType]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class TouchOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 timeout=None  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class UnlockOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 timeout=None  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class UpsertOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 expiry=None,  # type: Optional[timedelta]
                 preserve_expiry=False,  # type: Optional[bool]
                 durability=None,  # type: Optional[DurabilityType]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


# Sub-document Operations

class LookupInOptionsBase(OptionsBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 access_deleted=None  # type: Optional[bool]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class MutateInOptionsBase(OptionsDurabilityBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 cas=0,          # type: Optional[int]
                 durability=None,  # type: Optional[DurabilityType]
                 store_semantics=None,  # type: Optional[StoreSemantics]
                 access_deleted=None,  # type: Optional[bool]
                 preserve_expiry=None  # type: Optional[bool]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

"""

Python SDK Streaming Operation Options Base Classes

"""

class QueryOptionsBase(dict):

    # @TODO: span
    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        read_only=None,  # type: Optional[bool]
        scan_consistency=None,  # type: Optional[QueryScanConsistency]
        adhoc=None,  # type: Optional[bool]
        client_context_id=None,  # type: Optional[str]
        max_parallelism=None,  # type: Optional[int]
        positional_parameters=None,  # type: Optional[Iterable[JSONType]]
        named_parameters=None,  # type: Optional[Dict[str, JSONType]]
        pipeline_batch=None,  # type: Optional[int]
        pipeline_cap=None,  # type: Optional[int]
        profile=None,  # type: Optional[QueryProfile]
        query_context=None,  # type: Optional[str]
        scan_cap=None,  # type: Optional[int]
        scan_wait=None,  # type: Optional[timedelta]
        metrics=None,  # type: Optional[bool]
        flex_index=None,  # type: Optional[bool]
        preserve_expiry=None,  # type: Optional[bool]
        consistent_with=None,  # type: Optional[MutationState]
        send_to_node=None,  # type: Optional[str]
        raw=None,  # type: Optional[Dict[str,Any]]
        span=None,  # type: Optional[Any]
        serializer=None  # type: Optional[Serializer]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


"""

Couchbase Python SDK constrained integer classes

"""


AcceptableInts = Union[ctypes.c_int64, ctypes.c_uint64, int]


class ConstrainedIntBase():
    def __init__(self, value):
        """
        A signed integer between cls.min() and cls.max() inclusive

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        self.value = type(self).verify_value(value)

    @classmethod
    def verify_value(cls, item  # type: AcceptableInts
                     ):
        # type: (...) -> int
        value = getattr(item, 'value', item)
        if not isinstance(value, int) or not (cls.min() <= value <= cls.max()):
            raise InvalidArgumentException(
                "Integer in range {} and {} inclusiverequired".format(cls.min(), cls.max()))
        return value

    @classmethod
    def is_valid(cls,
                 item  # type: AcceptableInts
                 ):
        return isinstance(item, cls)

    def __neg__(self):
        return -self.value

    # Python 3.8 deprecated the implicit conversion to integers using __int__
    # use __index__ instead
    # still needed for Python 3.7
    def __int__(self):
        return self.value

    # __int__ falls back to __index__
    def __index__(self):
        return self.value

    def __add__(self, other):
        if not (self.min() <= (self.value + int(other)) <= self.max()):
            raise InvalidArgumentException(
                "{} + {} would be out of range {}-{}".format(self.value, other, self.min(), self.min()))

    @classmethod
    def max(cls):
        raise NotImplementedError()

    @classmethod
    def min(cls):
        raise NotImplementedError()

    def __str__(self):
        return "{cls_name} with value {value}".format(
            cls_name=type(self), value=self.value)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.value == other.value

    def __gt__(self, other):
        return self.value > other.value

    def __lt__(self, other):
        return self.value < other.value


class SignedInt64Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        A signed integer between -0x8000000000000000 and +0x7FFFFFFFFFFFFFFF inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super().__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return -0x8000000000000000


class UnsignedInt32Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        An unsigned integer between 0x00000000 and +0x80000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super().__init__(value)

    @classmethod
    def max(cls):
        return 0x00000000

    @classmethod
    def min(cls):
        return 0x80000000


class UnsignedInt64Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        An unsigned integer between 0x0000000000000000 and +0x8000000000000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super().__init__(value)

    @classmethod
    def min(cls):
        return 0x0000000000000000

    @classmethod
    def max(cls):
        return 0x8000000000000000


class DeltaValueBase(ConstrainedIntBase):
    def __init__(self,
                 value  # type: AcceptableInts
                 ):
        """
        A non-negative integer between 0 and +0x7FFFFFFFFFFFFFFF inclusive.
        Used as an argument for :meth:`Collection.increment` and :meth:`Collection.decrement`

        :param value: the value to initialise this with.

        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super().__init__(value)

    @ classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @ classmethod
    def min(cls):
        return 0