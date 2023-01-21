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

from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Union)

import grpc

from protostellar.proto.couchbase.query import v1_pb2

from couchbase._utils import (JSONType,
                              timedelta_as_microseconds,
                              to_microseconds,
                              timedelta_as_timestamp)
from couchbase.exceptions import AlreadyQueriedException, InvalidArgumentException
from couchbase.n1ql import QueryScanConsistency, QueryMetaData, QueryProfile, QueryStatus, QueryWarning
from couchbase.serializer import DefaultJsonSerializer, Serializer
from couchbase.tracing import CouchbaseSpan

from protostellar.options import QueryOptions

if TYPE_CHECKING:
    from protostellar.mutation_state import MutationState  # noqa: F401

def to_protostellar_query_scan_consistency(consistency # type: QueryScanConsistency
    ) -> v1_pb2.QueryRequest.QueryScanConsistency:
    if consistency == QueryScanConsistency.REQUEST_PLUS:
        return v1_pb2.QueryRequest.QueryScanConsistency.REQUEST_PLUS
    elif consistency == QueryScanConsistency.NOT_BOUNDED:
        return v1_pb2.QueryRequest.QueryScanConsistency.NOT_BOUNDED
    else:
        raise InvalidArgumentException(f'Invalid QueryScanConsistency for Protostellar: {consistency}')

def to_protostellar_query_profile_mode(mode # type: QueryProfile
    ) -> v1_pb2.QueryRequest.QueryProfileMode:
    if mode == QueryProfile.OFF:
        return v1_pb2.QueryRequest.QueryProfileMode.OFF
    elif mode == QueryProfile.PHASES:
        return v1_pb2.QueryRequest.QueryProfileMode.PHASES
    elif mode == QueryProfile.TIMINGS:
        return v1_pb2.QueryRequest.QueryProfileMode.TIMINGS
    else:
        raise InvalidArgumentException(f'Invalid QueryProfileMode for Protostellar: {mode}')

def from_protostellar_metadata_status(status # type: v1_pb2.QueryResponse.MetaDataStatus
    ) -> QueryStatus:
    if status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.RUNNING:
        return QueryStatus.RUNNING
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.SUCCESS:
        return QueryStatus.SUCCESS
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.ERRORS:
        return QueryStatus.ERRORS
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.COMPLETED:
        return QueryStatus.COMPLETED
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.STOPPED:
        return QueryStatus.STOPPED
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.TIMEOUT:
        return QueryStatus.TIMEOUT
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.CLOSED:
        return QueryStatus.CLOSED
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.FATAL:
        return QueryStatus.FATAL
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.ABORTED:
        return QueryStatus.ABORTED
    elif status == v1_pb2.QueryResponse.MetaData.MetaDataStatus.UNKNOWN:
        return QueryStatus.UNKNOWN

class N1QLQuery:

    # empty transform will skip updating the attribute when creating an
    # N1QLQuery object
    _VALID_OPTS = {
        "timeout": {"timeout": lambda x: x},
        "read_only": {"read_only": lambda x: x},
        "scan_consistency": {"consistency": lambda x: x},
        "consistent_with": {"consistent_with": lambda x: x},
        "adhoc": {"prepared": lambda x: x},
        "client_context_id": {"client_context_id": lambda x: x},
        "max_parallelism": {"max_parallelism": lambda x: x},
        "pipeline_batch": {"pipeline_batch": lambda x: x},
        "pipeline_cap": {"pipeline_cap": lambda x: x},
        "profile": {"profile_mode": lambda x: x},
        "query_context": {"query_context": lambda x: x},
        "raw": {"raw": lambda x: x},
        "scan_cap": {"scan_cap": lambda x: x},
        "scan_wait": {"scan_wait": timedelta_as_timestamp},
        "metrics": {"metrics": lambda x: x},
        "flex_index": {"flex_index": lambda x: x},
        "preserve_expiry": {"preserve_expiry": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "positional_parameters": {},
        "named_parameters": {},
        "span": {},
    }

    def __init__(self, bucket_name, scope_name, query, *args, **kwargs):

        self._params = {
            'bucket_name': bucket_name,
            'scope_name': scope_name,
            "statement": query,
        }
        self._serializer = DefaultJsonSerializer()
        self._raw = None
        if args:
            self._add_pos_args(*args)
        if kwargs:
            self._set_named_args(**kwargs)

    def _set_named_args(self, **kv):
        """
        Set a named parameter in the query. The named field must
        exist in the query itself.

        :param kv: Key-Value pairs representing values within the
            query. These values should be stripped of their leading
            `$` identifier.

        """
        arg_dict = self._params.setdefault("named_parameters", {})
        arg_dict.update(kv)

    def _add_pos_args(self, *args):
        """
        Set values for *positional* placeholders (``$1,$2,...``)

        :param args: Values to be used
        """
        arg_array = self._params.setdefault("positional_parameters", [])
        arg_array.extend(args)

    def set_option(self, name, value):
        """
        Set a raw option in the query. This option is encoded
        as part of the query parameters without any client-side
        verification. Use this for settings not directly exposed
        by the Python client.

        :param name: The name of the option
        :param value: The value of the option
        """
        self._params[name] = value



    @property
    def params(self) -> Dict[str, Any]:
        params = {}
        params.update(self._params)

        raw = params.pop('raw', None)
        if raw:
            params['raw'] = {f'{k}': self._serializer.serialize(v) for k, v in raw.items()}

        positional_args = params.pop('positional_parameters', None)
        if positional_args:
            params['positional_parameters'] = [self._serializer.serialize(arg) for arg in positional_args]

        named_params = params.pop('named_parameters', None)
        if named_params:
            params['named_parameters'] = {f'${k}': self._serializer.serialize(v) for k, v in named_params.items()}

        scan_consistency = params.pop('scan_consistency', None)
        if scan_consistency:
            if scan_consistency != QueryScanConsistency.AT_PLUS:
                params['scan_consitency'] = to_protostellar_query_scan_consistency(scan_consistency)
        
        profile_mode = params.pop('profile_mode', None)
        if profile_mode:
            params['profile_mode'] = to_protostellar_query_profile_mode(profile_mode)

        tuning_options = {}
        max_parallelism = params.pop('max_parallelism', None)
        if max_parallelism:
            tuning_options['max_parallelism'] = max_parallelism

        pipeline_batch = params.pop('pipeline_batch', None)
        if pipeline_batch:
            tuning_options['pipeline_batch'] = pipeline_batch

        pipeline_cap = params.pop('pipeline_cap', None)
        if pipeline_cap:
            tuning_options['pipeline_cap'] = pipeline_cap

        scan_wait = params.pop('scan_wait', None)
        if scan_wait:
            tuning_options['scan_wait'] = scan_wait

        scan_cap = params.pop('scan_cap', None)
        if scan_cap:
            tuning_options['scan_cap'] = scan_cap

        metrics = params.pop('metrics', None)
        if metrics is not None and metrics is False:
            tuning_options['disable_metrics'] = True

        if len(tuning_options) > 0:
            params['tuning_options'] = tuning_options

        return params

    @property
    def metrics(self) -> bool:
        return self._params.get('metrics', False)

    @metrics.setter
    def metrics(self, value  # type: bool
                ) -> None:
        self.set_option('metrics', value)

    @property
    def statement(self) -> str:
        return self._params['statement']

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

    @property
    def readonly(self) -> bool:
        return self._params.get('read_only', False)

    @readonly.setter
    def readonly(self, value  # type: bool
                 ) -> None:
        self._params['read_only'] = value

    @property
    def consistency(self) -> QueryScanConsistency:
        value = self._params.get(
            'scan_consistency', None
        )
        if value is None and 'mutation_state' in self._params:
            return QueryScanConsistency.AT_PLUS
        if value is None:
            return QueryScanConsistency.NOT_BOUNDED
        if isinstance(value, str):
            return QueryScanConsistency.REQUEST_PLUS if value == 'request_plus' else QueryScanConsistency.NOT_BOUNDED

    @consistency.setter
    def consistency(self, value  # type: Union[QueryScanConsistency, str]
                    ) -> None:
        invalid_argument = False
        if 'mutation_state' not in self._params:
            if isinstance(value, QueryScanConsistency):
                if value == QueryScanConsistency.AT_PLUS:
                    invalid_argument = True
                else:
                    self.set_option('scan_consistency', value.value)
            elif isinstance(value, str) and value in [sc.value for sc in QueryScanConsistency]:
                if value == QueryScanConsistency.AT_PLUS.value:
                    invalid_argument = True
                else:
                    self.set_option('scan_consistency', value)
            else:
                raise InvalidArgumentException(message=("Excepted consistency to be either of type "
                                                        "QueryScanConsistency or str representation "
                                                        "of QueryScanConsistency"))

        if invalid_argument:
            raise InvalidArgumentException(message=("Cannot set consistency to AT_PLUS.  Use "
                                                    "consistent_with instead or set consistency "
                                                    "to NOT_BOUNDED or REQUEST_PLUS"))

    @property
    def consistent_with(self):
        return {
            'consistency': self.consistency,
            'scan_vectors': self._params.get('mutation_state', None)
        }

    @consistent_with.setter
    def consistent_with(self, value  # type: MutationState
                        ):
        """
        Indicate that the query should be consistent with one or more
        mutations.

        :param value: The state of the mutations it should be consistent
            with.
        :type state: :class:`~.protostellar.mutation_state.MutationState`
        """
        if self.consistency != QueryScanConsistency.NOT_BOUNDED:
            raise TypeError(
                'consistent_with not valid with other consistency options')

        # avoid circular import
        from protostellar.mutation_state import MutationState  # noqa: F811
        if not (isinstance(value, MutationState) and len(value._sv) > 0):
            raise TypeError('Passed empty or invalid state')
        # 3.x SDK had to set the consistency, SN takes care of that for us
        self._params.pop('scan_consistency', None)
        self.set_option('mutation_state', list(value._sv))

    @property
    def adhoc(self) -> bool:
        return self._params.get('adhoc', True)

    @adhoc.setter
    def adhoc(self, value  # type: bool
              ) -> None:
        self.set_option('adhoc', value)

    @property
    def client_context_id(self) -> Optional[str]:
        return self._params.get('client_context_id', None)

    @client_context_id.setter
    def client_context_id(self, value  # type: str
                          ) -> None:
        self.set_option('client_context_id', value)

    @property
    def max_parallelism(self) -> Optional[int]:
        return self._params.get('max_parallelism', None)

    @max_parallelism.setter
    def max_parallelism(self, value  # type: int
                        ) -> None:
        self.set_option('max_parallelism', value)

    @property
    def pipeline_batch(self) -> Optional[int]:
        return self._params.get('pipeline_batch', None)

    @pipeline_batch.setter
    def pipeline_batch(self, value  # type: int
                       ) -> None:
        self.set_option('pipeline_batch', value)

    @property
    def pipeline_cap(self) -> Optional[int]:
        return self._params.get('pipeline_cap', None)

    @pipeline_cap.setter
    def pipeline_cap(self, value  # type: int
                     ) -> None:
        self.set_option('pipeline_cap', value)

    @property
    def profile(self) -> QueryProfile:
        value = self._params.get(
            'profile_mode', None
        )

        if value is None:
            return QueryProfile.OFF
        if isinstance(value, str):
            if value == 'off':
                return QueryProfile.OFF
            elif value == 'phases':
                return QueryProfile.PHASES
            else:
                return QueryProfile.TIMINGS

    @profile.setter
    def profile(self, value  # type: Union[QueryProfile, str]
                ) -> None:
        if isinstance(value, QueryProfile):
            self.set_option('profile_mode', value.value)
        elif isinstance(value, str) and value in [pm.value for pm in QueryProfile]:
            self.set_option('profile_mode', value)
        else:
            raise InvalidArgumentException(message=("Excepted profile to be either of type "
                                                    "QueryProfile or str representation of QueryProfile"))

    @property
    def query_context(self) -> Optional[str]:
        return self._params.get('scope_qualifier', None)

    @query_context.setter
    def query_context(self, value  # type: str
                      ) -> None:
        self.set_option('scope_qualifier', value)

    @property
    def send_to_node(self) -> Optional[str]:
        return self._params.get('send_to_node', None)

    @send_to_node.setter
    def send_to_node(self, value  # type: str
                     ) -> None:
        self.set_option('send_to_node', value)

    @property
    def scan_cap(self) -> Optional[int]:
        return self._params.get('scan_cap', None)

    @scan_cap.setter
    def scan_cap(self, value  # type: int
                 ) -> None:
        self.set_option('scan_cap', value)

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
    def flex_index(self) -> bool:
        return self._params.get('flex_index', False)

    @flex_index.setter
    def flex_index(self, value  # type: bool
                   ) -> None:
        self.set_option('flex_index', value)

    @property
    def preserve_expiry(self) -> bool:
        return self._params.get('preserve_expiry', False)

    @preserve_expiry.setter
    def preserve_expiry(self, value  # type: bool
                        ) -> None:
        self.set_option('preserve_expiry', value)

    @property
    def raw(self) -> Optional[Dict[str, Any]]:
        return self._params.get('raw', None)

    @raw.setter
    def raw(self, value  # type: Dict[str, Any]
            ) -> None:
        if not isinstance(value, dict):
            raise TypeError("Raw option must be of type Dict[str, Any].")
        for k in value.keys():
            if not isinstance(k, str):
                raise TypeError("key for raw value must be str")
        self.set_option('raw', value)

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
    def serializer(self) -> Optional[Serializer]:
        return self._params.get('serializer', None)

    @serializer.setter
    def serializer(self, value  # type: Serializer
                   ):
        if not issubclass(value.__class__, Serializer):
            raise InvalidArgumentException(message='Serializer should implement Serializer interface.')
        self.set_option('serializer', value)

    @classmethod
    def create_query_object(cls, bucket_name, scope_name, statement, *options, **kwargs):
        # lets make a copy of the options, and update with kwargs...
        opt = QueryOptions()
        # TODO: is it possible that we could have [QueryOptions, QueryOptions, ...]??
        #       If so, why???
        opts = list(options)
        for o in opts:
            if isinstance(o, QueryOptions):
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

        query = cls(bucket_name, scope_name, statement, *positional_parameters, **named_parameters)
        # now lets try to setup the options.
        # but for now we will use the existing N1QLQuery.  Could be we can
        # add to it, etc...

        # default to false on metrics
        # query.metrics = args.get("metrics", False)

        for k, v in ((k, args[k]) for k in (args.keys() & cls._VALID_OPTS)):
            for target, transform in cls._VALID_OPTS[k].items():
                setattr(query, target, transform(v))
        return query

class N1QLRequest:
    def __init__(self,
                 service,
                 query_params,
                 row_factory=lambda x: x,
                 **kwargs
                 ):

        self._query_service = service
        self._query_params = query_params
        self.row_factory = row_factory
        self._streaming_result = None
        self._started_streaming = False
        self._done_streaming = False
        self._raw_rows = []
        self._raw_metadata = None
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

    @classmethod
    def generate_n1ql_request(cls, service, query_params, row_factory=lambda x: x, **kwargs):
        return cls(service, query_params, row_factory=row_factory, **kwargs)

    def execute(self):
        return [r for r in list(self)]

    def metadata(self):
        return self._metadata

    def _submit_query(self, **kwargs):
        if self.done_streaming:
            return

        self._started_streaming = True
        req = v1_pb2.QueryRequest(**self._query_params)
        self._streaming_result = self._query_service.Query(req)

    def _set_metadata(self):
        if self._raw_metadata is None:
            return
        
        metadata = {
            'request_id':self._raw_metadata.request_id,
            'client_context_id':self._raw_metadata.client_context_id,
            'status': self._raw_metadata.status,
            'signature': json.loads(self._raw_metadata.signature.decode('utf-8')),
        }
        
        if self._raw_metadata.HasField('metrics'):
            metrics = {
                'elapsed_time': self._raw_metadata.metrics.elapsed_time.ToNanoseconds(),
                'execution_time': self._raw_metadata.metrics.execution_time.ToNanoseconds(),
                'result_count': self._raw_metadata.metrics.result_count,
                'result_size': self._raw_metadata.metrics.result_size,
                'mutation_count': self._raw_metadata.metrics.mutation_count,
                'sort_count': self._raw_metadata.metrics.sort_count,
                'error_count': self._raw_metadata.metrics.error_count,
                'warning_count': self._raw_metadata.metrics.warning_count,
            }
            metadata['metrics'] = metrics

        if len(self._raw_metadata.warnings) > 0:
            metadata['warnings'] = []
            for warning in self._raw_metadata.warnings:
                metadata['warnings'].append(QueryWarning({'code': warning.code, 'message': warning.message}))

        if self._raw_metadata.HasField('profile'):
            metadata['profile'] = json.loads(self._raw_metadata.profile.decode('utf-8'))

        self._metadata = QueryMetaData({'metadata': metadata})


    def __iter__(self):
        if self.done_streaming:
            raise AlreadyQueriedException()

        if not self.started_streaming:
            self._submit_query()

        return self

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        res = next(self._streaming_result)
        if res.HasField('meta_data'):
            self._raw_metadata = res.meta_data
        if len(res.rows) > 0:
            for r in res.rows:
                self._raw_rows.append(r)

        row = self._raw_rows.pop(0)
        return self.serializer.deserialize(row)

    def __next__(self):
        try:
            return self._get_next_row()
        except StopIteration:
            self._done_streaming = True
            self._set_metadata()
            raise
        except grpc.RpcError as grpc_err:
            # ex = parse_proto_exception(grpc_err)
            raise grpc_err
        except Exception as ex:
            raise ex
