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

from datetime import timedelta

import pytest

from tests.test_environment import (EnvironmentFeatures, TestEnvironment)
from tests.query_test_environment import QueryTestEnvironment
from new_couchbase.mutation_state import MutationState, MutationToken
from new_couchbase.exceptions import InvalidArgumentException
from new_couchbase.n1ql import QueryProfile, QueryScanConsistency
from new_couchbase.options import QueryOptions

class QueryParamsTestSuite:
    TEST_MANIFEST = [
        'test_consistent_with', # CP
        'test_encoded_consistency', # CP
        'test_params_adhoc', # CP
        'test_params_base', # CP
        'test_params_client_context_id', # CP
        'test_params_flex_index', # CP
        'test_params_max_parallelism', # CP
        'test_params_metrics', # CP
        'test_params_pipeline_batch', # CP
        'test_params_pipeline_cap', # CP
        'test_params_preserve_expiry', # CP
        'test_params_profile', # CP
        'test_params_query_context', # C
        'test_params_readonly', # CP
        'test_params_scan_cap', # CP
        'test_params_scan_consistency', # CP
        'test_params_scan_wait', # CP
        'test_params_serializer', # CP
        'test_params_timeout', # CP
    ]

    def test_consistent_with(self, n1ql_query):

        q_str = 'SELECT * FROM default'
        ms = MutationState()
        mt = MutationToken(token={
            'partition_id': 42,
            'partition_uuid': 3004,
            'sequence_number': 3,
            'bucket_name': 'default'
        })
        ms._add_scanvec(mt)
        q_opts = QueryOptions(consistent_with=ms)
        query = n1ql_query.create_query_object(q_str, q_opts)

        # couchbase++ will set scan_consistency, so params should be
        # None, but the prop should return AT_PLUS
        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, set)
        assert len(q_mt) == 1
        assert q_mt.pop() == mt

        # Ensure no dups
        ms = MutationState()
        mt1 = MutationToken(token={
            'partition_id': 42,
            'partition_uuid': 3004,
            'sequence_number': 3,
            'bucket_name': 'default'
        })
        ms._add_scanvec(mt)
        ms._add_scanvec(mt1)
        q_opts = QueryOptions(consistent_with=ms)
        query = n1ql_query.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, set)
        assert len(q_mt) == 1
        assert q_mt.pop() == mt

        # Try with a second bucket
        ms = MutationState()
        mt2 = MutationToken(token={
            'partition_id': 42,
            'partition_uuid': 3004,
            'sequence_number': 3,
            'bucket_name': 'default1'
        })
        ms._add_scanvec(mt)
        ms._add_scanvec(mt2)
        q_opts = QueryOptions(consistent_with=ms)
        query = n1ql_query.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) is None
        assert query.consistency == QueryScanConsistency.AT_PLUS

        q_mt = query.params.get('mutation_state', None)
        assert isinstance(q_mt, set)
        assert len(q_mt) == 2
        assert next((m for m in q_mt if m == mt2), None) is not None

    def test_params_base(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions()
        query = n1ql_query.create_query_object(q_str, q_opts)
        assert query.params == base_opts

    def test_params_client_context_id(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(client_context_id='test-string-id')
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['client_context_id'] = 'test-string-id'
        assert query.params == exp_opts

    def test_params_flex_index(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(flex_index=True)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['flex_index'] = True
        assert query.params == exp_opts

    def test_params_preserve_expiry(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(preserve_expiry=True)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['preserve_expiry'] = True
        assert query.params == exp_opts

        q_opts = QueryOptions(preserve_expiry=False)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['preserve_expiry'] = False
        assert query.params == exp_opts

        # if not set, the prop will return False, but preserve_expiry should
        # not be in the params
        query = n1ql_query.create_query_object(q_str)
        exp_opts = base_opts.copy()
        assert query.params == exp_opts
        assert query.preserve_expiry is False

    def test_params_profile(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(profile=QueryProfile.PHASES)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['profile_mode'] = QueryProfile.PHASES.value
        assert query.params == exp_opts
        assert query.profile == QueryProfile.PHASES

    def test_params_query_context(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(query_context='bucket.scope')
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scope_qualifier'] = 'bucket.scope'
        assert query.params == exp_opts

    def test_params_serializer(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        from couchbase.serializer import DefaultJsonSerializer

        # serializer
        serializer = DefaultJsonSerializer()
        q_opts = QueryOptions(serializer=serializer)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['serializer'] = serializer
        assert query.params == exp_opts



class ClassicQueryParamsTests(QueryParamsTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            return callable(getattr(QueryParamsTestSuite, meth)) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(QueryParamsTestSuite) if valid_test_method(meth)]
        compare = set(QueryParamsTestSuite.TEST_MANIFEST).difference(method_list)
        return len(compare) == 0

    @pytest.fixture(scope='class')
    def n1ql_query(self):
        from new_couchbase.classic.core.n1ql import N1QLQuery
        return N1QLQuery

    @pytest.fixture(scope='class')
    def base_opts(self):
        return {'statement': 'SELECT * FROM default',
                'metrics': False}

    def test_encoded_consistency(self, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        query = n1ql_query.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == QueryScanConsistency.REQUEST_PLUS.value
        assert query.consistency == QueryScanConsistency.REQUEST_PLUS

        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.NOT_BOUNDED)
        query = n1ql_query.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == QueryScanConsistency.NOT_BOUNDED.value
        assert query.consistency == QueryScanConsistency.NOT_BOUNDED

        # cannot set scan_consistency to AT_PLUS, need to use consistent_with to do that
        with pytest.raises(InvalidArgumentException):
            q_opts = QueryOptions(scan_consistency=QueryScanConsistency.AT_PLUS)
            query = n1ql_query.create_query_object(q_str, q_opts)

    def test_params_adhoc(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(adhoc=False)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['adhoc'] = False
        assert query.params == exp_opts

    def test_params_max_parallelism(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(max_parallelism=5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['max_parallelism'] = 5
        assert query.params == exp_opts

    def test_params_metrics(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(metrics=True)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['metrics'] = True
        assert query.params == exp_opts

    def test_params_pipeline_batch(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(pipeline_batch=5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['pipeline_batch'] = 5
        assert query.params == exp_opts

    def test_params_pipeline_cap(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(pipeline_cap=5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['pipeline_cap'] = 5
        assert query.params == exp_opts

    def test_params_readonly(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(read_only=True)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['readonly'] = True
        assert query.params == exp_opts

    def test_params_scan_cap(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_cap=5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scan_cap'] = 5
        assert query.params == exp_opts

    def test_params_scan_consistency(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scan_consistency'] = QueryScanConsistency.REQUEST_PLUS.value
        assert query.params == exp_opts
        assert query.consistency == QueryScanConsistency.REQUEST_PLUS

    def test_params_scan_wait(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_wait=timedelta(seconds=30))
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['scan_wait'] = 30000000
        assert query.params == exp_opts

    def test_params_timeout(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(timeout=timedelta(seconds=20))
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        assert query.params == exp_opts

        q_opts = QueryOptions(timeout=20)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20000000
        assert query.params == exp_opts

        q_opts = QueryOptions(timeout=25.5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 25500000
        assert query.params == exp_opts

class ProtostellarQueryParamsTests(QueryParamsTestSuite):
    SKIP_LIST = {
        'test_params_query_context': 'Feature Unvailable',
    }

    @pytest.fixture(scope='function', autouse=True)
    def can_run_test(self, request):
        test_name = request.function.__name__
        if test_name in ProtostellarQueryParamsTests.SKIP_LIST:
            pytest.skip(ProtostellarQueryParamsTests.SKIP_LIST[test_name])


    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            return callable(getattr(QueryParamsTestSuite, meth)) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(QueryParamsTestSuite) if valid_test_method(meth)]
        compare = set(QueryParamsTestSuite.TEST_MANIFEST).difference(method_list)
        return len(compare) == 0

    @pytest.fixture(scope='class')
    def n1ql_query(self):
        from new_couchbase.protostellar.n1ql import N1QLQuery
        return N1QLQuery

    @pytest.fixture(scope='class')
    def base_opts(self):
        return {'statement': 'SELECT * FROM default'}

    def test_encoded_consistency(self, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        query = n1ql_query.create_query_object(q_str, q_opts)

        from new_couchbase.protostellar.proto.couchbase.query import v1_pb2

        assert query.params.get('scan_consistency', None) == v1_pb2.QueryRequest.QueryScanConsistency.REQUEST_PLUS
        assert query.consistency == QueryScanConsistency.REQUEST_PLUS

        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.NOT_BOUNDED)
        query = n1ql_query.create_query_object(q_str, q_opts)

        assert query.params.get('scan_consistency', None) == v1_pb2.QueryRequest.QueryScanConsistency.NOT_BOUNDED
        assert query.consistency == QueryScanConsistency.NOT_BOUNDED

        # cannot set scan_consistency to AT_PLUS, need to use consistent_with to do that
        with pytest.raises(InvalidArgumentException):
            q_opts = QueryOptions(scan_consistency=QueryScanConsistency.AT_PLUS)
            query = n1ql_query.create_query_object(q_str, q_opts)

    def test_params_adhoc(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(adhoc=False)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['prepared'] = False
        assert query.params == exp_opts

    def test_params_max_parallelism(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(max_parallelism=5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['tuning_options'] = {'max_parallelism': 5}
        assert query.params == exp_opts

    def test_params_metrics(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(metrics=True)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        assert query.params == exp_opts
        assert query.metrics is True

        q_opts = QueryOptions(metrics=False)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['tuning_options'] = {'disable_metrics': True}
        assert query.params == exp_opts
        assert query.metrics is False

    def test_params_pipeline_batch(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(pipeline_batch=5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['tuning_options'] = {'pipeline_batch': 5}
        assert query.params == exp_opts

    def test_params_pipeline_cap(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(pipeline_cap=5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['tuning_options'] = {'pipeline_cap': 5}
        assert query.params == exp_opts

    def test_params_profile(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(profile=QueryProfile.PHASES)
        query = n1ql_query.create_query_object(q_str, q_opts)

        from new_couchbase.protostellar.proto.couchbase.query import v1_pb2

        exp_opts = base_opts.copy()
        exp_opts['profile_mode'] = v1_pb2.QueryRequest.QueryProfileMode.PHASES
        assert query.params == exp_opts
        assert query.profile == QueryProfile.PHASES

    def test_params_readonly(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(read_only=True)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['read_only'] = True
        assert query.params == exp_opts

    def test_params_scan_cap(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_cap=5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['tuning_options'] = {'scan_cap': 5}
        assert query.params == exp_opts

    def test_params_scan_consistency(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        query = n1ql_query.create_query_object(q_str, q_opts)

        from new_couchbase.protostellar.proto.couchbase.query import v1_pb2

        exp_opts = base_opts.copy()
        exp_opts['scan_consistency'] = v1_pb2.QueryRequest.QueryScanConsistency.REQUEST_PLUS
        assert query.params == exp_opts
        assert query.consistency == QueryScanConsistency.REQUEST_PLUS

    def test_params_scan_wait(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(scan_wait=timedelta(seconds=30))
        query = n1ql_query.create_query_object(q_str, q_opts)

        from google.protobuf.duration_pb2 import Duration

        exp_opts = base_opts.copy()
        d = Duration()
        d.FromSeconds(30)
        exp_opts['tuning_options'] = {'scan_wait': d}
        assert query.params == exp_opts


    def test_params_timeout(self, base_opts, n1ql_query):
        q_str = 'SELECT * FROM default'
        q_opts = QueryOptions(timeout=timedelta(seconds=20))
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20
        assert query.params == exp_opts

        q_opts = QueryOptions(timeout=20)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 20
        assert query.params == exp_opts

        q_opts = QueryOptions(timeout=25.5)
        query = n1ql_query.create_query_object(q_str, q_opts)

        exp_opts = base_opts.copy()
        exp_opts['timeout'] = 25.5
        assert query.params == exp_opts