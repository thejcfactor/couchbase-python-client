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

from datetime import timedelta
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Optional,
                    Union)

import grpc
from google.protobuf.duration_pb2 import Duration

from new_couchbase.protostellar.proto.couchbase.query import v1_pb2

from new_couchbase.protostellar._utils import timedelta_as_duration, to_seconds
from new_couchbase.exceptions import AlreadyQueriedException, InvalidArgumentException
from new_couchbase.serializer import DefaultJsonSerializer, Serializer
# from couchbase.tracing import CouchbaseSpan
from new_couchbase.common.n1ql import (N1QLQueryBase,
                                       QueryMetaData,
                                       QueryProfile,
                                       QueryScanConsistency,
                                       QueryStatus,
                                       QueryWarning)
from new_couchbase.options import QueryOptions

# from protostellar.options import QueryOptions

if TYPE_CHECKING:
    from protostellar.mutation_state import MutationState  # noqa: F401

def to_protostellar_query_scan_consistency(consistency # type: Union[str, QueryScanConsistency]
    ) -> v1_pb2.QueryRequest.QueryScanConsistency:
    if isinstance(consistency, str):
        consistency = QueryScanConsistency(consistency)
    if consistency == QueryScanConsistency.REQUEST_PLUS:
        return v1_pb2.QueryRequest.QueryScanConsistency.REQUEST_PLUS
    elif consistency == QueryScanConsistency.NOT_BOUNDED:
        return v1_pb2.QueryRequest.QueryScanConsistency.NOT_BOUNDED
    else:
        raise InvalidArgumentException(f'Invalid QueryScanConsistency for Protostellar: {consistency}')

def to_protostellar_query_profile_mode(mode # type: Union[str, QueryProfile]
    ) -> v1_pb2.QueryRequest.QueryProfileMode:
    if isinstance(mode, str):
        mode = QueryProfile(mode)
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

class N1QLQuery(N1QLQueryBase):

    _VALID_OPTS = {
        "bucket_name": {"bucket_name": lambda x: x},
        "timeout": {"timeout": to_seconds},
        "read_only": {"readonly": lambda x: x},
        "scan_consistency": {"consistency": lambda x: x},
        "consistent_with": {"consistent_with": lambda x: x},
        "adhoc": {"adhoc": lambda x: x},
        "client_context_id": {"client_context_id": lambda x: x},
        "max_parallelism": {"max_parallelism": lambda x: x},
        "pipeline_batch": {"pipeline_batch": lambda x: x},
        "pipeline_cap": {"pipeline_cap": lambda x: x},
        "profile": {"profile": lambda x: x},
        # "query_context": {"query_context": lambda x: x},
        "raw": {"raw": lambda x: x},
        "scan_cap": {"scan_cap": lambda x: x},
        "scan_wait": {"scan_wait": timedelta_as_duration},
        "scope_name": {"scope_name": lambda x: x},
        "metrics": {"metrics": lambda x: x},
        "flex_index": {"flex_index": lambda x: x},
        "preserve_expiry": {"preserve_expiry": lambda x: x},
        "serializer": {"serializer": lambda x: x},
        "positional_parameters": {},
        "named_parameters": {},
        # "span": {},
    }

    def __init__(self, query, *args, **kwargs):

        self._params = {
            "statement": query,
        }
        bucket_name = kwargs.pop('bucket_name', None)
        if bucket_name:
            self.bucket_name = bucket_name
        scope_name = kwargs.pop('scope_name', None)
        if scope_name:
            self.scope_name = scope_name
        self._serializer = DefaultJsonSerializer()
        self._raw = None
        if args:
            self._add_pos_args(*args)
        if kwargs:
            self._set_named_args(**kwargs)

    @property
    def adhoc(self) -> bool:
        return self._params.get('prepared', True)

    @adhoc.setter
    def adhoc(self, value  # type: bool
              ) -> None:
        self.set_option('prepared', value)

    @property
    def bucket_name(self) -> Optional[str]:
        return self._params.get('bucket_name', None)

    @adhoc.setter
    def bucket_name(self, value  # type: str
              ) -> None:
        self.set_option('bucket_name', value)

    @property
    def params(self) -> Dict[str, Any]:
        params = {}
        params.update(self._params)

        prepared = params.pop('adhoc', None)
        if prepared:
            tuning_options['prepared'] = prepared

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
                params['scan_consistency'] = to_protostellar_query_scan_consistency(scan_consistency)
        
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
    def readonly(self) -> bool:
        return self._params.get('read_only', False)

    @readonly.setter
    def readonly(self, value  # type: bool
                 ) -> None:
        self._params['read_only'] = value

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
        elif isinstance(value, Duration):
            self.set_option('scan_wait', value)
        else:
            # if using the setter, need to validate/transform timedelta, otherwise, just add the value
            if 'scan_wait' in self._params:
                value = timedelta_as_duration(value)

            self.set_option('scan_wait', value)

    @property
    def scope_name(self) -> Optional[str]:
        return self._params.get('scope_name', None)

    @adhoc.setter
    def scope_name(self, value  # type: str
              ) -> None:
        self.set_option('scope_name', value)

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
            total_secs = to_seconds(value)
            self.set_option('timeout', total_secs)

    @classmethod
    def create_query_object(cls, statement, *options, **kwargs):
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

        query = cls(statement, *positional_parameters, **named_parameters)
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
        self._grpc_done_streaming = False
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

    def _add_raw_rows(self):
        if self._grpc_done_streaming is True:
            return

        try:
            res = next(self._streaming_result)
            if res.HasField('meta_data'):
                self._raw_metadata = res.meta_data

            if len(res.rows) > 0:
                for r in res.rows:
                    self._raw_rows.append(r)
        except StopIteration:
            self._grpc_done_streaming = True

    def _get_next_row(self):
        if self.done_streaming is True:
            return

        row = None
        while not self._grpc_done_streaming or len(self._raw_rows) > 0:
            if len(self._raw_rows) == 0:
                self._add_raw_rows()
                continue

            row = self._raw_rows.pop(0)
            if row:
                break

        # if the row is None, the grpc stream should be exhausted AND we should have exhausted all our raw_rows
        if row is None:
            raise StopIteration
        
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