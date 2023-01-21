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
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Union)

from new_couchbase.common._utils import (timedelta_as_microseconds,
                              to_microseconds)
from new_couchbase.exceptions import InvalidArgumentException
from new_couchbase.classic.exceptions import ErrorMapper
from new_couchbase.classic.exceptions import exception as CouchbaseBaseException
from new_couchbase.common.options import QueryOptionsBase
from new_couchbase.options import QueryOptions
from couchbase.pycbc_core import n1ql_query
from new_couchbase.serializer import DefaultJsonSerializer, Serializer
from new_couchbase.common.n1ql import N1QLQueryBase, QueryMetaData, QueryProfile, QueryScanConsistency
from couchbase.tracing import CouchbaseSpan

if TYPE_CHECKING:
    from couchbase.mutation_state import MutationState  # noqa: F401


class N1QLQuery(N1QLQueryBase):

    # empty transform will skip updating the attribute when creating an
    # N1QLQuery object
    _VALID_OPTS = {
        "timeout": {"timeout": lambda x: x},
        "read_only": {"readonly": lambda x: x},
        "scan_consistency": {"consistency": lambda x: x},
        "consistent_with": {"consistent_with": lambda x: x},
        "adhoc": {"adhoc": lambda x: x},
        "client_context_id": {"client_context_id": lambda x: x},
        "max_parallelism": {"max_parallelism": lambda x: x},
        "pipeline_batch": {"pipeline_batch": lambda x: x},
        "pipeline_cap": {"pipeline_cap": lambda x: x},
        "profile": {"profile": lambda x: x},
        "query_context": {"query_context": lambda x: x},
        "raw": {"raw": lambda x: x},
        "scan_cap": {"scan_cap": lambda x: x},
        "scan_wait": {"scan_wait": timedelta_as_microseconds},
        "metrics": {"metrics": lambda x: x},
        "flex_index": {"flex_index": lambda x: x},
        "preserve_expiry": {"preserve_expiry": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "positional_parameters": {},
        "named_parameters": {},
        "span": {"span": lambda x: x}
    }

    def __init__(self, query, *args, **kwargs):

        self._params = {"statement": query}
        self._serializer = DefaultJsonSerializer()
        self._raw = None
        if args:
            self._add_pos_args(*args)
        if kwargs:
            self._set_named_args(**kwargs)

    @property
    def adhoc(self) -> bool:
        return self._params.get('adhoc', True)

    @adhoc.setter
    def adhoc(self, value  # type: bool
              ) -> None:
        self.set_option('adhoc', value)

    @property
    def params(self) -> Dict[str, Any]:
        params = self._params

        # couchbase++ wants all args JSONified,
        # For now encode to bytes to make couchbase::json_string <--> std::vector<std::byte> easier
        raw = params.pop('raw', None)
        if raw:
            params['raw'] = {f'{k}': self._serializer.serialize(v) for k, v in raw.items()}

        positional_args = params.pop('positional_parameters', None)
        if positional_args:
            params['positional_parameters'] = [self._serializer.serialize(arg) for arg in positional_args]

        named_params = params.pop('named_parameters', None)
        if named_params:
            params['named_parameters'] = {f'${k}': self._serializer.serialize(v) for k, v in named_params.items()}
        return params


    @property
    def query_context(self) -> Optional[str]:
        return self._params.get('scope_qualifier', None)

    @query_context.setter
    def query_context(self, value  # type: str
                      ) -> None:
        self.set_option('scope_qualifier', value)

    @property
    def readonly(self) -> bool:
        return self._params.get('readonly', False)

    @readonly.setter
    def readonly(self, value  # type: bool
                 ) -> None:
        self._params['readonly'] = value

    @property
    def scan_wait(self) -> Optional[float]:
        value = self._params.get('scan_wait', None)
        if not value:
            return None
        value = value[:-1]
        return float(value)

    @scan_wait.setter
    def scan_wait(self, value  # type: timedelta
                  ) -> None:
        if not value:
            self._params.pop('scan_wait', 0)
        else:
            # if using the setter, need to validate/transform timedelta, otherwise, just add the value
            if 'scan_wait' in self._params:
                value = timedelta_as_microseconds(value)

            self.set_option('scan_wait', value)

    @property
    def send_to_node(self) -> Optional[str]:
        return self._params.get('send_to_node', None)

    @send_to_node.setter
    def send_to_node(self, value  # type: str
                     ) -> None:
        self.set_option('send_to_node', value)

    @property
    def span(self) -> Optional[CouchbaseSpan]:
        return self._params.get('span', None)

    @span.setter
    def span(self, value  # type CouchbaseSpan
             ) -> None:
        if not issubclass(value.__class__, CouchbaseSpan):
            raise InvalidArgumentException(message='Span should implement CouchbaseSpan interface')
        self.set_option('span', value)

    @property
    def timeout(self) -> Optional[float]:
        value = self._params.get('timeout', None)
        if not value:
            return None
        value = value[:-1]
        return float(value)

    @timeout.setter
    def timeout(self, value  # type: Union[timedelta,float,int]
                ) -> None:
        if not value:
            self._params.pop('timeout', 0)
        else:
            total_us = to_microseconds(value)
            self.set_option('timeout', total_us)


    @classmethod
    def create_query_object(cls, statement, *options, **kwargs):
        # lets make a copy of the options, and update with kwargs...
        opt = QueryOptions()
        # TODO: is it possible that we could have [QueryOptions, QueryOptions, ...]??
        #       If so, why???
        opts = list(options)
        for o in opts:
            if isinstance(o, (QueryOptions, QueryOptionsBase)):
                opt = o
                opts.remove(o)
        args = opt.copy()
        args.update(kwargs)

        # now lets get positional parameters.  Actual positional
        # params OVERRIDE positional_parameters
        positional_parameters = args.pop("positional_parameters", [])
        if opts and len(opts) > 0:
            positional_parameters = opts

        # now the named parameters.  NOTE: all the kwargs that are
        # not VALID_OPTS must be named parameters, and the kwargs
        # OVERRIDE the list of named_parameters
        new_keys = list(filter(lambda x: x not in cls._VALID_OPTS, args.keys()))
        named_parameters = args.pop("named_parameters", {})
        for k in new_keys:
            named_parameters[k] = args[k]

        query = cls(statement, *positional_parameters, **named_parameters)
        # now lets try to setup the options.
        # but for now we will use the existing N1QLQuery.  Could be we can
        # add to it, etc...

        # default to false on metrics
        query.metrics = args.get("metrics", False)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query


class N1QLRequestCore:
    def __init__(self,
                 connection,
                 query_params,
                 row_factory=lambda x: x,
                 **kwargs
                 ):

        self._connection = connection
        self._query_params = query_params
        self.row_factory = row_factory
        self._streaming_result = None
        self._started_streaming = False
        self._done_streaming = False
        self._metadata = None
        self._default_serializer = kwargs.pop('default_serializer', DefaultJsonSerializer())
        self._serializer = None

    @property
    def params(self) -> Dict[str, Any]:
        return self._query_params

    @property
    def serializer(self) -> Serializer:
        if self._serializer:
            return self._serializer

        serializer = self.params.get('serializer', None)
        if not serializer:
            serializer = self._default_serializer

        self._serializer = serializer
        return self._serializer

    @property
    def started_streaming(self) -> bool:
        return self._started_streaming

    @property
    def done_streaming(self) -> bool:
        return self._done_streaming

    def metadata(self):
        # @TODO:  raise if query isn't complete?
        return self._metadata

    def _set_metadata(self, query_response):
        if isinstance(query_response, CouchbaseBaseException):
            raise ErrorMapper.build_exception(query_response)

        self._metadata = QueryMetaData(query_response.raw_result.get('value', None))

    def _submit_query(self, **kwargs):
        if self.done_streaming:
            return

        self._started_streaming = True
        n1ql_kwargs = {
            'conn': self._connection,
            'query_args': self.params,
        }

        # this is for txcouchbase...
        callback = kwargs.pop('callback', None)
        if callback:
            n1ql_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            n1ql_kwargs['errback'] = errback

        self._streaming_result = n1ql_query(**n1ql_kwargs)
