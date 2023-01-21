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

from datetime import datetime, timedelta
from time import time

import pytest

from new_couchbase.exceptions import (CasMismatchException,
                                      DocumentExistsException,
                                      DocumentNotFoundException)
from new_couchbase.options import (GetOptions,
                                    InsertOptions,
                                    RemoveOptions,
                                    ReplaceOptions,
                                    UpsertOptions)
from new_couchbase.result import (ExistsResult,
                                  GetResult,
                                  MutationResult)

from tests.test_environment import (CollectionType, EnvironmentFeatures, KVPair, TestEnvironment)

class CollectionTestSuite:
    FIFTY_YEARS = 50 * 365 * 24 * 60 * 60
    THIRTY_DAYS = 30 * 24 * 60 * 60

    TEST_MANIFEST = [
        'test_document_expiry_values',
        'test_does_not_exists',
        'test_exists',
        'test_expiry_really_expires',
        'test_get', # CP
        'test_get_after_lock',
        'test_get_all_replicas',
        'test_get_all_replicas_fail',
        'test_get_all_replicas_results',
        'test_get_and_lock',
        'test_get_and_lock_replace_with_cas',
        'test_get_and_touch',
        'test_get_and_touch_no_expire',
        'test_get_any_replica',
        'test_get_any_replica_fail',
        'test_get_fails', # CP
        'test_get_options', # CP
        'test_get_with_expiry',
        'test_insert', # CP
        'test_insert_document_exists', # CP
        'test_project',
        'test_project_bad_path',
        'test_project_project_not_list',
        'test_project_too_many_projections',
        'test_remove', # CP
        'test_remove_fail', # CP
        'test_replace', # CP
        'test_replace_fail', # CP
        'test_replace_preserve_expiry',
        'test_replace_preserve_expiry_fail',
        'test_replace_preserve_expiry_not_used',
        'test_replace_with_cas', # C
        'test_touch',
        'test_touch_no_expire',
        'test_unlock',
        'test_unlock_wrong_cas',
        'test_upsert', # CP
        'test_upsert_preserve_expiry',
        'test_upsert_preserve_expiry_not_used',
    ]

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            return callable(getattr(CollectionTestSuite, meth)) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(CollectionTestSuite) if valid_test_method(meth)]
        compare = set(CollectionTestSuite.TEST_MANIFEST).difference(method_list)
        return len(compare) == 0

    @pytest.fixture(scope="class")
    def check_preserve_expiry_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('preserve_expiry',
                                                        cb_env.server_version_short,
                                                        cb_env.mock_server_type)

    @pytest.fixture(scope="class")
    def check_xattr_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('xattr',
                                                        cb_env.server_version_short,
                                                        cb_env.mock_server_type)

    @pytest.fixture(name="default_kvp")
    def get_existing_doc(self, cb_env) -> KVPair:
        key, value = cb_env.get_existing_doc()
        yield KVPair(key, value)

    @pytest.fixture(name="new_kvp")
    def get_new_doc(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_doc()
        yield KVPair(key, value)

    # @pytest.mark.flaky(reruns=5, reruns_delay=1)
    def test_exists(self, cb_env, default_kvp):
        key = default_kvp.key
        result = cb_env.collection.exists(key)
        assert isinstance(result, ExistsResult)
        assert result.exists is True

    def test_does_not_exists(self, cb_env):
        result = cb_env.collection.exists(TestEnvironment.NOT_A_KEY)
        assert isinstance(result, ExistsResult)
        assert result.exists is False

    def test_get(self, cb_env, default_kvp):
        key = default_kvp.key
        value = default_kvp.value
        result = cb_env.collection.get(key)
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_options(self, cb_env, default_kvp):
        key = default_kvp.key
        value = default_kvp.value
        result = cb_env.collection.get(key, GetOptions(
            timeout=timedelta(seconds=2), with_expiry=False))
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_fails(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.get(TestEnvironment.NOT_A_KEY)

    # @pytest.mark.usefixtures("check_xattr_supported")
    # def test_get_with_expiry(self, cb_env, new_kvp):
    #     cb = cb_env.collection
    #     key = new_kvp.key
    #     value = new_kvp.value
    #     cb.upsert(key, value, UpsertOptions(expiry=timedelta(seconds=1000)))

    #     expiry_path = "$document.exptime"
    #     res = cb_env.try_n_times(10, 3, cb.lookup_in, key, (SD.get(expiry_path, xattr=True),))
    #     expiry = res.content_as[int](0)
    #     assert expiry is not None
    #     assert expiry > 0
    #     expires_in = (datetime.fromtimestamp(expiry) - datetime.now()).total_seconds()
    #     # when running local, this can be be up to 1050, so just make sure > 0
    #     assert expires_in > 0

    def test_insert(self, cb_env, new_kvp):
        key = new_kvp.key
        value = new_kvp.value
        result = cb_env.collection.insert(key, value, InsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb_env.collection.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_insert_document_exists(self, cb_env, default_kvp):
        key = default_kvp.key
        value = default_kvp.value
        with pytest.raises(DocumentExistsException):
            cb_env.collection.insert(key, value)

    def test_remove(self, cb_env, default_kvp):
        key = default_kvp.key
        result = cb_env.collection.remove(key)
        assert isinstance(result, MutationResult)

        with pytest.raises(DocumentNotFoundException):
            TestEnvironment.try_n_times_till_exception(3,
                                              1,
                                              cb_env.collection.get,
                                              key,
                                              expected_exceptions=(DocumentNotFoundException,),
                                              raise_exception=True)

    def test_remove_fail(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.remove(TestEnvironment.NOT_A_KEY)

    def test_replace(self, cb_env, default_kvp):
        key = default_kvp.key
        value = default_kvp.value
        result = cb_env.collection.replace(key, value, ReplaceOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_replace_with_cas(self, cb_env, default_kvp, new_kvp):
        key = default_kvp.key
        value1 = new_kvp.value
        result = cb_env.collection.get(key)
        old_cas = result.cas
        result = cb_env.collection.replace(key, value1, ReplaceOptions(cas=old_cas))
        assert isinstance(result, MutationResult)
        assert result.cas != old_cas

        # try same cas again, must fail.
        with pytest.raises(CasMismatchException):
            cb_env.collection.replace(key, value1, ReplaceOptions(cas=old_cas))

    def test_replace_fail(self, cb_env):
        with pytest.raises(DocumentNotFoundException):
            cb_env.collection.replace(TestEnvironment.NOT_A_KEY, {"some": "content"})

    def test_upsert(self, cb_env, default_kvp):
        key = default_kvp.key
        value = default_kvp.value
        result = cb_env.collection.upsert(key, value, UpsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

class ClassicCollectionTests(CollectionTestSuite):
    # @pytest.fixture(scope='function', autouse=True)
    # def can_run_test(self, request):
    #     test_name = request.function.__name__


    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, couchbase_config, test_manifest_validated, request):
        env_args = {
            'test_suite': __name__,
            'couchbase_config': couchbase_config,
            'coll_type': request.param,
            'manage_buckets': True
        }
        cb_env = TestEnvironment.get_environment(**env_args)

        TestEnvironment.try_n_times(3, 5, cb_env.load_data)
        yield cb_env
        TestEnvironment.try_n_times(3, 5, cb_env.purge_data)


class ProtostellarCollectionTests(CollectionTestSuite):
    SKIP_LIST = {
        'test_does_not_exists': 'ING-58',
        'test_exists': 'ING-58',
        'test_replace_with_cas': 'ING-355',
    }
    @pytest.fixture(scope='function', autouse=True)
    def can_run_test(self, request):
        test_name = request.function.__name__
        if test_name in ProtostellarCollectionTests.SKIP_LIST:
            pytest.skip(ProtostellarCollectionTests.SKIP_LIST[test_name])


    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, couchbase_config, test_manifest_validated, request):
        env_args = {
            'test_suite': __name__,
            'couchbase_config': couchbase_config,
            'coll_type': request.param,
            'manage_buckets': True
        }
        cb_env = TestEnvironment.get_environment(**env_args)

        TestEnvironment.try_n_times(3, 5, cb_env.load_data)
        yield cb_env
        TestEnvironment.try_n_times(3, 5, cb_env.purge_data)