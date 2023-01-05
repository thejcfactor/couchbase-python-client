import base64

from copy import copy
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Optional, overload, TYPE_CHECKING

import grpc

from couchbase._utils import timedelta_as_microseconds, validate_bool, validate_int

from protostellar.durability import DurabilityParser

if TYPE_CHECKING:
    from protostellar.transcoder import Transcoder
    from protostellar.durability import DurabilityType

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
                 **kwargs # type: Dict[str, Any]
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
                 **kwargs # type: Dict[str, Any]
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
                 **kwargs # type: Dict[str, Any]
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

class UpsertOptions(dict):
    _VALID_OPTS = {
        "timeout": {"timeout": timedelta_as_microseconds},
        "expiry": {"expiry": timedelta_as_microseconds},
        "durability": {"durability": DurabilityParser.parse_durability},
        "transcoder": {"transcoder": lambda x: x},
    }

    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        expiry=None,  # type: Optional[bool]
        durability=None,  # type: Optional[DurabilityType]
        transcoder=None  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self,
                 **kwargs # type: Dict[str, Any]
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


