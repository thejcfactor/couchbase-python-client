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

import pytest

from tests.test_environment import (EnvironmentFeatures, TestEnvironment)
from tests.query_test_environment import QueryTestEnvironment

from new_couchbase.options import QueryOptions

class QueryTestSuite:
    TEST_MANIFEST = [
        'test_bad_query',
        'test_mixed_named_parameters',
        'test_mixed_positional_parameters',
        'test_preserve_expiry',
        'test_query_error_context',
        'test_query_in_thread',
        'test_query_metadata',
        'test_query_raw_options',
        'test_query_with_metrics',
        'test_query_with_profile',
        'test_simple_query',  # CP
        'test_simple_query_explain',  # CP
        'test_simple_query_prepared',  # CP
        'test_simple_query_with_named_params',  # CP
        'test_simple_query_with_named_params_in_options',
        'test_simple_query_with_positional_params',  # CP
        'test_simple_query_with_positional_params_in_options',
        'test_simple_query_without_options_with_kwargs_named_params',
        'test_simple_query_without_options_with_kwargs_positional_params',
    ]

    @pytest.fixture(scope="class")
    def check_preserve_expiry_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('preserve_expiry',
                                                        cb_env.server_version_short,
                                                        cb_env.mock_server_type)

    def test_simple_query(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._core_result._request.params.get('adhoc', None) is None

    def test_simple_query_explain(self, cb_env):
        result = cb_env.cluster.query(f"EXPLAIN SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(metrics=True))
        rows = []
        for r in result.rows():
            rows.append(r)

        assert len(rows) == 1
        assert 'plan' in rows[0]
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None

    def test_simple_query_prepared(self, cb_env):
        # @TODO(CXXCBC-174)
        if cb_env.server_version_short < 6.5:
            pytest.skip(f'Skipped on server versions < 6.5 (using {cb_env.server_version_short}). Pending CXXCBC-174')
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(adhoc=False, metrics=True))
        cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None
        assert result._core_result._request.params.get('adhoc', None) is False

    def test_simple_query_with_positional_params(self, cb_env):
        result = cb_env.cluster.query(
            f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 2", 'United%')
        cb_env.assert_rows(result, 2)

    def test_simple_query_with_named_params(self, cb_env):
        result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 2",
                                      country='United%')
        cb_env.assert_rows(result, 2)

class ClassicQueryTests(QueryTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            return callable(getattr(QueryTestSuite, meth)) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(QueryTestSuite) if valid_test_method(meth)]
        compare = set(QueryTestSuite.TEST_MANIFEST).difference(method_list)
        return len(compare) == 0

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, couchbase_config, test_manifest_validated):
        env_args = {
            'test_suite': __name__,
            'couchbase_config': couchbase_config,
            'manage_query_indexes': True
        }
        cb_env = QueryTestEnvironment.get_environment(**env_args)

        TestEnvironment.try_n_times(3, 5, cb_env.load_data)
        cb_env.setup()
        yield cb_env
        TestEnvironment.try_n_times(3, 5, cb_env.purge_data)
        cb_env.teardown()

class ProtostellarQueryTests(QueryTestSuite):
    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            return callable(getattr(QueryTestSuite, meth)) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(QueryTestSuite) if valid_test_method(meth)]
        compare = set(QueryTestSuite.TEST_MANIFEST).difference(method_list)
        return len(compare) == 0

    @pytest.fixture(scope='class', name='cb_env')
    def couchbase_test_environment(self, couchbase_config, test_manifest_validated):
        env_args = {
            'test_suite': __name__,
            'couchbase_config': couchbase_config,
            'manage_query_indexes': True
        }
        cb_env = QueryTestEnvironment.get_environment(**env_args)

        TestEnvironment.try_n_times(3, 5, cb_env.load_data)
        cb_env.setup()
        yield cb_env
        TestEnvironment.try_n_times(3, 5, cb_env.purge_data)
        cb_env.teardown()

    def test_simple_query(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2")
        cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        # if adhoc is not set, it should be None
        assert result._request.params.get('prepared', None) is None

    def test_simple_query_explain(self, cb_env):
        # @TODO(jc) - make adhoc false by default?
        result = cb_env.scope.query(f"EXPLAIN SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(adhoc=False, metrics=True))
        rows = []
        for r in result.rows():
            rows.append(r)

        assert len(rows) == 1
        assert 'plan' in rows[0]
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None

    def test_simple_query_prepared(self, cb_env):
        # @TODO(CXXCBC-174)
        if cb_env.server_version_short < 6.5:
            pytest.skip(f'Skipped on server versions < 6.5 (using {cb_env.server_version_short}). Pending CXXCBC-174')
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2",
                                      QueryOptions(adhoc=False, metrics=True))
        cb_env.assert_rows(result, 2)
        assert result.metadata() is not None
        assert result.metadata().metrics() is not None
        assert result._request.params.get('prepared', None) is False

    def test_simple_query_with_positional_params(self, cb_env):
        result = cb_env.scope.query(
            f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $1 LIMIT 2", 'United%')
        cb_env.assert_rows(result, 2)

    def test_simple_query_with_named_params(self, cb_env):
        result = cb_env.scope.query(f"SELECT * FROM `{cb_env.bucket.name}` WHERE country LIKE $country LIMIT 2",
                                      country='United%')
        cb_env.assert_rows(result, 2)