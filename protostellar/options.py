import base64
from copy import copy
from dataclasses import dataclass, field
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    overload)

import grpc

from couchbase._utils import (timedelta_as_microseconds,
                              timedelta_as_timestamp,
                              validate_bool,
                              validate_int)
from protostellar.durability import DurabilityParser
from protostellar.subdocument import StoreSemantics

if TYPE_CHECKING:
    from protostellar.durability import DurabilityType
    from protostellar.transcoder import Transcoder


@dataclass
class ConnectOptions:
    username: str = None
    password: str = None
    root_certificate: str = None
    client_certificate: str = None
    private_key: str = None

    def grpc_call_credentials(self) -> grpc.CallCredentials:
        token = base64.encode(f'{self.username}:{self.password}')
        auth_str = f'authorization: Basic {token}'
        return grpc.access_token_call_credentials(auth_str)


class ExistsOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "with_expiry": {"view_threshold": validate_bool},
        "project": {"query_threshold": lambda x: x},
        "transcoder": {"transcoder": lambda x: x},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        with_expiry=None,  # type: Optional[bool]
        project=None,  # type: Optional[List[str]]
        transcoder=None  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAndLockOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "lock_time": {"lock_time": lambda x: int(x.total_seconds())},
        "transcoder": {"transcoder": lambda x: x},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        transcoder=None,  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAndTouchOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "expiry": {"expiry": timedelta_as_timestamp},
        "transcoder": {"transcoder": lambda x: x},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        transcoder=None,  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class InsertOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "expiry": {"expiry": timedelta_as_timestamp},
        "durability": {"durability": DurabilityParser.parse_durability},
        "transcoder": {"transcoder": lambda x: x},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        expiry=None,  # type: Optional[timedelta]
        durability=None,  # type: Optional[DurabilityType]
        transcoder=None  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class RemoveOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "cas": {"cas": validate_int},
        "durability": {"durability": DurabilityParser.parse_durability},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        cas=None,  # type: Optional[int]
        durability=None,  # type: Optional[DurabilityType]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ReplaceOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "cas": {"cas": validate_int},
        "expiry": {"expiry": timedelta_as_timestamp},
        "durability": {"durability": DurabilityParser.parse_durability},
        "transcoder": {"transcoder": lambda x: x},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        cas=None,  # type: Optional[int]
        expiry=None,  # type: Optional[timedelta]
        durability=None,  # type: Optional[DurabilityType]
        transcoder=None  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class TouchOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UnlockOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UpsertOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "expiry": {"expiry": timedelta_as_timestamp},
        "durability": {"durability": DurabilityParser.parse_durability},
        "transcoder": {"transcoder": lambda x: x},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        expiry=None,  # type: Optional[timedelta]
        durability=None,  # type: Optional[DurabilityType]
        transcoder=None  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class LookupInOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "expiry": {"expiry": timedelta_as_timestamp},
        "durability": {"durability": DurabilityParser.parse_durability},
        "access_deleted": {"access_deleted": validate_bool},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        expiry=None,  # type: Optional[timedelta]
        durability=None,  # type: Optional[DurabilityType]
        access_deleted=None,  # type: Optional[bool]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class MutateInOptions(dict):
    _VALID_OPTS = {
        "cas": {"cas": validate_int},
        "timeout": {"timeout": timedelta_as_microseconds},
        "durability": {"durability": DurabilityParser.parse_durability},
        "store_semantics": {"store_semantics": lambda x: x},
    }

    @overload
    def __init__(
        self,
        cas=None,  # type: Optional[int]
        timeout=None,  # type: Optional[timedelta]
        durability=None,  # type: Optional[DurabilityType]
        store_semantics=None,  # type: Optional[StoreSemantics]
    ):
        pass

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


def parse_options(options, **kwargs):
    tmp_opts = copy(options) if options else dict()
    tmp_opts.update(kwargs)
    final_opts = {}
    for k, v in tmp_opts.items():
        valid = options._VALID_OPTS.get(k, None)
        if valid is None:
            continue

        for out_key, transform in valid.items():
            converted = transform(v)
            if converted is not None:
                final_opts[out_key] = converted

    return final_opts
