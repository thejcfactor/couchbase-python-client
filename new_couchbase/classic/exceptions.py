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

import json
import re
import pathlib
import sys
from collections import defaultdict
from enum import Enum
from string import Template
from typing import (Any,
                    Dict,
                    Optional,
                    Set,
                    Union)

from couchbase.pycbc_core import exception
from new_couchbase.exceptions import (CouchbaseException,
                                      DocumentLockedException,
                                      KeyspaceNotFoundException,
                                      QueryIndexNotFoundException,
                                      ScopeNotFoundException,
                                      TemporaryFailException)

COUCHBASE_ROOT = pathlib.Path(__file__).parent.parent

class ErrorContext:
    def __init__(self, **kwargs):
        self._base = kwargs

    @property
    def last_dispatched_to(self) -> Optional[str]:
        return self._base.get("last_dispatched_to", None)

    @property
    def last_dispatched_from(self) -> Optional[str]:
        return self._base.get("last_dispatched_from", None)

    @property
    def retry_attempts(self) -> int:
        return self._base.get("retry_attempts", None)

    @property
    def retry_reasons(self) -> Set[str]:
        return self._base.get("retry_reasons", None)

    @staticmethod
    def from_dict(**kwargs):
        # type: (...) -> ErrorContext
        klass = kwargs.get("context_type", "ErrorContext")
        cl = getattr(sys.modules[__name__], klass)
        return cl(**kwargs)

    def _get_base(self):
        return self._base

    def __repr__(self):
        return f'ErrorContext({self._base})'

class HTTPErrorContext(ErrorContext):
    _HTTP_EC_KEYS = ["client_context_id", "method", "path", "http_status",
                     "http_body"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._http_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._HTTP_EC_KEYS}

    @property
    def method(self) -> Optional[str]:
        return self._http_err_ctx.get("method", None)

    @property
    def response_code(self) -> Optional[int]:
        return self._http_err_ctx.get("http_status", None)

    @property
    def path(self) -> Optional[str]:
        return self._http_err_ctx.get("path", None)

    @property
    def response_body(self) -> Optional[str]:
        return self._http_err_ctx.get("http_body", None)

    @property
    def client_context_id(self) -> Optional[str]:
        return self._http_err_ctx.get("client_context_id", None)

    def __repr__(self):
        return f'HTTPErrorContext({self._http_err_ctx})'

class AnalyticsErrorContext(HTTPErrorContext):
    _ANALYTICS_EC_KEYS = ["first_error_code", "first_error_message", "statement", "parameters"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._analytics_err_ctx = {k: v for k,
                                   v in kwargs.items() if k in self._ANALYTICS_EC_KEYS}

    @property
    def first_error_code(self) -> Optional[int]:
        return self._analytics_err_ctx.get("first_error_code", None)

    @property
    def first_error_message(self) -> Optional[str]:
        return self._analytics_err_ctx.get("first_error_message", None)

    @property
    def statement(self) -> Optional[str]:
        return self._analytics_err_ctx.get("statement", None)

    @property
    def parameters(self) -> Optional[str]:
        return self._analytics_err_ctx.get("parameters", None)

    def __repr__(self):
        return f'AnalyticsErrorContext({self._get_base()})'

class KeyValueErrorContext(ErrorContext):
    _KV_EC_KEYS = ['key', 'bucket_name', 'scope_name', 'collection_name',
                   'opaque', 'status_code', 'error_map_info', 'extended_error_info',
                   'retry_attempts', 'retry_reasons']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._kv_err_ctx = {k: v for k,
                            v in kwargs.items() if k in self._KV_EC_KEYS}

    @property
    def key(self) -> Optional[str]:
        return self._kv_err_ctx.get("key", None)

    @property
    def bucket_name(self) -> Optional[str]:
        return self._kv_err_ctx.get("bucket_name", None)

    @property
    def scope_name(self) -> Optional[str]:
        return self._kv_err_ctx.get("scope_name", None)

    @property
    def collection_name(self) -> Optional[str]:
        return self._kv_err_ctx.get("collection_name", None)

    def __repr__(self):
        return "KeyValueErrorContext:{}".format(self._kv_err_ctx)


class ManagementErrorContext(ErrorContext):
    _MGMT_EC_KEYS = ["client_context_id", "content", "path", "http_status"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._mgmt_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._MGMT_EC_KEYS}

    @property
    def response_code(self) -> Optional[int]:
        return self._mgmt_err_ctx.get("http_status", None)

    @property
    def path(self) -> Optional[str]:
        return self._mgmt_err_ctx.get("path", None)

    @property
    def content(self) -> Optional[str]:
        return self._mgmt_err_ctx.get("content", None)

    @property
    def client_context_id(self) -> Optional[str]:
        return self._mgmt_err_ctx.get("client_context_id", None)

    def __repr__(self):
        return f'ManagementErrorContext({self._mgmt_err_ctx})'


class QueryErrorContext(HTTPErrorContext):
    _QUERY_EC_KEYS = ["first_error_code", "first_error_message", "statement", "parameters"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._query_err_ctx = {k: v for k,
                               v in kwargs.items() if k in self._QUERY_EC_KEYS}

    @property
    def first_error_code(self) -> Optional[int]:
        return self._query_err_ctx.get("first_error_code", None)

    @property
    def first_error_message(self) -> Optional[str]:
        return self._query_err_ctx.get("first_error_message", None)

    @property
    def statement(self) -> Optional[str]:
        return self._query_err_ctx.get("statement", None)

    @property
    def parameters(self) -> Optional[str]:
        return self._query_err_ctx.get("parameters", None)

    def __repr__(self):
        return f'QueryErrorContext({self._get_base()})'


class SearchErrorContext(HTTPErrorContext):
    _SEARCH_EC_KEYS = ["index_name", "query", "parameters"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._search_err_ctx = {k: v for k,
                                v in kwargs.items() if k in self._SEARCH_EC_KEYS}

    @property
    def index_name(self) -> Optional[str]:
        return self._search_err_ctx.get("index_name", None)

    @property
    def query(self) -> Optional[str]:
        return self._search_err_ctx.get("query", None)

    @property
    def parameters(self) -> Optional[str]:
        return self._search_err_ctx.get("parameters", None)

    def __repr__(self):
        return f'SearchErrorContext({self._get_base()})'

class TransactionsErrorContext(ErrorContext):
    _TXN_EC_KEYS = ["failure_type"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.txn_err_ctx = {k: v for k,
                            v in kwargs.items() if k in self._TXN_EC_KEYS}

    @property
    def failure_type(self) -> Optional[str]:
        return self.txn_err_ctx.get("failure_type", None)

    @property
    def last_dispatched_to(self) -> Optional[str]:
        return None

    @property
    def last_dispatched_from(self) -> Optional[str]:
        return None

    @property
    def retry_attempts(self) -> int:
        return None

    @property
    def retry_reasons(self) -> Set[str]:
        return None

    def __str__(self):
        return f'TransactionsErrorContext{{{self.txn_err_ctx}}}'

class ViewErrorContext(HTTPErrorContext):
    _VIEW_EC_KEYS = ["design_document_name", "view_name", "query_string"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._view_err_ctx = {k: v for k,
                              v in kwargs.items() if k in self._VIEW_EC_KEYS}

    @property
    def design_document_name(self) -> Optional[str]:
        return self._view_err_ctx.get("design_document_name", None)

    @property
    def view_name(self) -> Optional[str]:
        return self._view_err_ctx.get("view_name", None)

    @property
    def query_string(self) -> Optional[str]:
        return self._view_err_ctx.get("query_string", None)

    def __repr__(self):
        return f'ViewErrorContext({self._get_base()})'


class SubdocumentErrorContext(KeyValueErrorContext):
    _SUBDOC_EC_KEYS = ["first_error_path", "first_error_index", "deleted"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._subdoc_err_ctx = {k: v for k,
                                v in kwargs.items() if k in self._SUBDOC_EC_KEYS}

    @property
    def first_error_path(self) -> Optional[str]:
        return self._subdoc_err_ctx.get("first_error_path", None)

    @property
    def first_error_index(self) -> Optional[int]:
        return self._subdoc_err_ctx.get("first_error_index", None)

    @property
    def deleted(self) -> bool:
        return self._subdoc_err_ctx.get("deleted", False)

    def __repr__(self):
        return f'SubdocumentErrorContext({self._get_base()})'


ErrorContextType = Union[AnalyticsErrorContext,
                         ErrorContext,
                         HTTPErrorContext,
                         KeyValueErrorContext,
                         ManagementErrorContext,
                         QueryErrorContext,
                         SearchErrorContext,
                         SubdocumentErrorContext,
                         TransactionsErrorContext,
                         ViewErrorContext]

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
    AmbiguousTimeoutException = 13
    UnAmbiguousTimeoutException = 14
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

KV_ERROR_CONTEXT_MAPPING = {'key_value_locked': DocumentLockedException,
                            'key_value_temporary_failure': TemporaryFailException}

QUERY_ERROR_MAPPING = {r'.*Keyspace not found.*': KeyspaceNotFoundException,
                       r'.*Scope not found.*': ScopeNotFoundException,
                       r'.*No index available.*': QueryIndexNotFoundException}

class ErrorMapper:

    @staticmethod
    def _process_mapping(compiled_map,  # type: Dict[str, CouchbaseException]
                         err_content  # type: str
                         ) -> Optional[CouchbaseException]:
        matches = None
        for pattern, exc_class in compiled_map.items():
            try:
                matches = pattern.match(err_content)
            except Exception:  # nosec
                pass
            if matches:
                return exc_class

        return None

    @staticmethod
    def _parse_kv_context(err_ctx,  # type: KeyValueErrorContext
                          mapping,  # type: Dict[str, CouchbaseException]
                          err_content=None  # type: str
                          ) -> Optional[CouchbaseException]:
        from couchbase._utils import is_null_or_empty

        compiled_map = {{str: re.compile}.get(
            type(k), lambda x: x)(k): v for k, v in mapping.items()}

        if not is_null_or_empty(err_content):
            exc_class = ErrorMapper._process_mapping(compiled_map, err_content)
            if exc_class is not None:
                return exc_class

        if err_ctx.retry_reasons is not None:
            for rr in err_ctx.retry_reasons:
                exc_class = ErrorMapper._process_mapping(compiled_map, rr)
                if exc_class is not None:
                    return exc_class

        return None

    @classmethod
    def build_exception(cls,
                        base_exc,  # type: exception
                        mapping=None,  # type: Dict[str, CouchbaseException]
                        ) -> CouchbaseException:
        exc_class = None
        err_ctx = None
        ctx = base_exc.error_context()
        if ctx is None:
            exc_class = PYCBC_ERROR_MAP.get(base_exc.err(), CouchbaseException)
            err_info = base_exc.error_info()
        else:
            err_ctx = ErrorContext.from_dict(**ctx)
            err_info = base_exc.error_info()

            # if isinstance(err_ctx, HTTPErrorContext):
            #     exc_class = ErrorMapper._parse_http_context(err_ctx, mapping, err_info=err_info)

            if isinstance(err_ctx, KeyValueErrorContext):
                if mapping is None:
                    mapping = KV_ERROR_CONTEXT_MAPPING
                exc_class = ErrorMapper._parse_kv_context(err_ctx, mapping)

            # if isinstance(err_ctx, QueryErrorContext):
            #     if mapping is None:
            #         mapping = QUERY_ERROR_MAPPING
            #     exc_class = ErrorMapper._parse_http_context(err_ctx, mapping)

        if exc_class is None:
            exc_class = PYCBC_ERROR_MAP.get(base_exc.err(), CouchbaseException)

        exc = exc_class(base=base_exc, exc_info=err_info, context=err_ctx)
        return exc