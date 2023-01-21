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

import json
import re
import pathlib
import sys
from collections import defaultdict
from enum import Enum
from string import Template
from typing import (Any,
                    Dict,
                    List,
                    Tuple,
                    Optional,
                    Set,
                    Union)
import grpc
from grpc import StatusCode

from new_couchbase.exceptions import (CouchbaseException,
                                        DocumentNotFoundException,
                                        DocumentExistsException,
                                        FeatureUnavailableException)

COUCHBASE_ROOT = pathlib.Path(__file__).parent.parent

class ExceptionMap(Enum):
    # RequestCanceledException = 2
    InvalidArgumentException = 3
    ServiceUnavailableException = 4
    # InternalServerFailureException = 5
    # AuthenticationException = 6
    # TemporaryFailException = 7
    # ParsingFailedException = 8
    CasMismatchException = 9
    # BucketNotFoundException = 10
    # AmbiguousTimeoutException = 13
    # UnAmbiguousTimeoutException = 14
    FeatureUnavailableException = 15
    # ScopeNotFoundException = 16
    # QueryIndexNotFoundException = 17
    # QueryIndexAlreadyExistsException = 18
    # RateLimitedException = 21
    # QuotaLimitedException = 22
    DocumentNotFoundException = 101
    DocumentUnretrievableException = 102
    DocumentLockedException = 103
    DocumentExistsException = 105
    # DurabilityInvalidLevelException = 107
    # DurabilityImpossibleException = 108
    # DurabilitySyncWriteAmbiguousException = 109
    # DurabilitySyncWriteInProgressException = 110
    # PathNotFoundException = 113
    # PathMismatchException = 114
    # InvalidValueException = 119
    # PathExistsException = 123
    # DatasetNotFoundException = 303
    # DataverseNotFoundException = 304
    # DatasetAlreadyExistsException = 305
    # DataverseAlreadyExistsException = 306
    # DesignDocumentNotFoundException = 502
    InternalSDKException = 5000
    # HTTPException = 5001
    # UnsuccessfulOperationException = 5002

PYCBC_ERROR_MAP = {e.value: getattr(sys.modules['new_couchbase.exceptions'], e.name) for e in ExceptionMap}

class ErrorMapper:
    @classmethod
    def build_exception(cls,
                        ex,  # type: grpc.RpcError
                        metadata, # type: List[Tuple(str, str)]
                        mapping=None,  # type: Dict[str, CouchbaseException]
                        ) -> CouchbaseException:
        exc_class = None
        err_ctx = None
        # if need traceback info:
        # import traceback
        # tb = ''.join(traceback.format_stack())
        if ex.code() == StatusCode.NOT_FOUND:
            kwargs = {
                'message': ex.details(),
                'error_code': ex.code().value[0],
                'exc_info': {'inner_cause': ex.debug_error_string()}
            }
            return DocumentNotFoundException(**kwargs)
        elif ex.code() == StatusCode.ALREADY_EXISTS:
            kwargs = {
                'message': ex.details(),
                'error_code': ex.code().value[0],
                'exc_info': {'inner_cause': ex.debug_error_string()}
            }
            return DocumentExistsException(**kwargs)
        elif ex.code() == StatusCode.UNIMPLEMENTED:
            kwargs = {
                'message': ex.details(),
                'error_code': ex.code().value[0],
                'exc_info': {'inner_cause': ex.debug_error_string()}
            }
            return FeatureUnavailableException(**kwargs)