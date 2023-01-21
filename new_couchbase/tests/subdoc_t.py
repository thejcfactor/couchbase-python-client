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

import pytest

import new_couchbase.subdocument as SD
from new_couchbase.exceptions import (DocumentExistsException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException,
                                  InvalidValueException,
                                  PathExistsException,
                                  PathMismatchException,
                                  PathNotFoundException)
from new_couchbase.options import GetOptions, MutateInOptions
from new_couchbase.result import (GetResult,
                              LookupInResult,
                              MutateInResult)

from tests.test_environment import (CollectionType,
                                    EnvironmentFeatures,
                                    KVPair,
                                    TestEnvironment)

class SubDocumentTestSuite:
    TEST_MANIFEST = [
        'test_array_add_unique',
        'test_array_add_unique_create_parents',
        'test_array_add_unique_fail',
        'test_array_append',
        'test_array_append_create_parents',
        'test_array_append_multi_insert',
        'test_array_as_document',
        'test_array_insert',
        'test_array_insert_multi_insert',
        'test_array_prepend',
        'test_array_prepend_create_parents',
        'test_array_prepend_multi_insert',
        'test_count',
        'test_decrement',
        'test_decrement_create_parents',
        'test_increment',
        'test_increment_create_parents',
        'test_insert_create_parents',
        'test_lookup_in_multiple_specs', # C
        'test_lookup_in_one_path_not_found', # C
        'test_lookup_in_simple_exists', # C
        'test_lookup_in_simple_exists_bad_path', # C
        'test_lookup_in_simple_get', # C
        'test_lookup_in_simple_get_spec_as_list', # C
        'test_lookup_in_simple_long_path', # C
        'test_mutate_in_expiry',
        'test_mutate_in_insert_semantics',
        'test_mutate_in_insert_semantics_fail',
        'test_mutate_in_insert_semantics_kwargs',
        'test_mutate_in_preserve_expiry',
        'test_mutate_in_preserve_expiry_fails',
        'test_mutate_in_preserve_expiry_not_used',
        'test_mutate_in_remove',
        'test_mutate_in_replace_semantics',
        'test_mutate_in_replace_semantics_fail',
        'test_mutate_in_replace_semantics_kwargs',
        'test_mutate_in_simple', # C
        'test_mutate_in_simple_spec_as_list', # C
        'test_mutate_in_store_semantics_fail',
        'test_mutate_in_upsert_semantics',
        'test_mutate_in_upsert_semantics_kwargs',
        'test_upsert_create_parents',
    ]

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            return callable(getattr(SubDocumentTestSuite, meth)) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(SubDocumentTestSuite) if valid_test_method(meth)]
        compare = set(SubDocumentTestSuite.TEST_MANIFEST).difference(method_list)
        return len(compare) == 0

    @pytest.fixture(scope="class")
    def check_xattr_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('xattr',
                                                        cb_env.server_version_short,
                                                        cb_env.mock_server_type)

    @pytest.fixture(name="default_kvp")
    def get_existing_doc(self, cb_env) -> KVPair:
        key, value = cb_env.get_existing_doc(has_field='geo')
        yield KVPair(key, value)

    @pytest.fixture(name="default_kvp_by_type")
    def get_existing_doc_by_type(self, cb_env, request) -> KVPair:
        key, value = cb_env.get_existing_doc_by_type(request.param)
        yield KVPair(key, value)

    @pytest.fixture(name="new_kvp")
    def get_new_doc(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_doc()
        yield KVPair(key, value)

    @pytest.mark.usefixtures("check_xattr_supported")
    @pytest.mark.parametrize('default_kvp_by_type', [['landmark', 'hotel']], indirect=['default_kvp_by_type'])
    def test_lookup_in_multiple_specs(self, cb_env, default_kvp_by_type):
        key, value = default_kvp_by_type
        result = cb_env.collection.lookup_in(key, (SD.get(
            "$document.exptime", xattr=True), SD.exists("geo"), SD.get("geo"), SD.get("geo.accuracy"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[int](0) == 0
        assert result.exists(1) is True
        assert result.content_as[dict](2) == value["geo"]
        assert result.content_as[str](3) == value["geo"]["accuracy"]

    def test_lookup_in_one_path_not_found(self, cb_env, default_kvp):
        key, _ = default_kvp
        result = cb_env.collection.lookup_in(
            key, (SD.exists("geo"), SD.exists("qzzxy"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        assert result.exists(1) is False
        with pytest.raises(DocumentNotFoundException):
            result.content_as[bool](0)
        with pytest.raises(PathNotFoundException):
            result.content_as[bool](1)

    def test_lookup_in_simple_exists(self, cb_env, default_kvp):
        key, _ = default_kvp
        result = cb_env.collection.lookup_in(key, (SD.exists("geo"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        # no value content w/ EXISTS operation
        with pytest.raises(DocumentNotFoundException):
            result.content_as[bool](0)

    def test_lookup_in_simple_exists_bad_path(self, cb_env, default_kvp):
        key, _ = default_kvp
        result = cb_env.collection.lookup_in(key, (SD.exists("qzzxy"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0) is False
        with pytest.raises(PathNotFoundException):
            result.content_as[bool](0)

    def test_lookup_in_simple_get(self, cb_env, default_kvp):
        key, value = default_kvp
        result = cb_env.collection.lookup_in(key, (SD.get("geo"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[dict](0) == value["geo"]

    def test_lookup_in_simple_get_spec_as_list(self, cb_env, default_kvp):
        key = default_kvp.key
        value = default_kvp.value
        result = cb_env.collection.lookup_in(key, [SD.get("geo")])
        assert isinstance(result, LookupInResult)
        assert result.content_as[dict](0) == value["geo"]

    def test_lookup_in_simple_long_path(self, cb_env, new_kvp):
        key, value = new_kvp
        # add longer path to doc
        value["long_path"] = {"a": {"b": {"c": "yo!"}}}
        cb_env.collection.upsert(key, value)
        TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)
        result = cb_env.collection.lookup_in(
            key, (SD.get("long_path.a.b.c"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[str](0) == value["long_path"]["a"]["b"]["c"]

    @pytest.mark.parametrize('default_kvp_by_type', [['airport']], indirect=['default_kvp_by_type'])
    def test_mutate_in_simple(self, cb_env, default_kvp_by_type):
        key, value = default_kvp_by_type
        # cb_env.collection.upsert(key, value)
        # TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)

        result = cb_env.collection.mutate_in(key,
                              (SD.upsert("city", "New City"),
                               SD.replace("faa", "CTY")))

        value["city"] = "New City"
        value["faa"] = "CTY"

        def cas_matches(cb, new_cas):
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        TestEnvironment.try_n_times(10, 3, cas_matches, cb_env.collection, result.cas)

        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]

    @pytest.mark.parametrize('default_kvp_by_type', [['airport']], indirect=['default_kvp_by_type'])
    def test_mutate_in_simple_spec_as_list(self, cb_env, default_kvp_by_type):
        key = default_kvp_by_type.key
        value = default_kvp_by_type.value
        cb_env.collection.upsert(key, value)
        TestEnvironment.try_n_times(10, 3, cb_env.collection.get, key)

        result = cb_env.collection.mutate_in(key,
                              [SD.upsert("city", "New City"),
                               SD.replace("faa", "CTY")])

        value["city"] = "New City"
        value["faa"] = "CTY"

        def cas_matches(cb, new_cas):
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        TestEnvironment.try_n_times(10, 3, cas_matches, cb_env.collection, result.cas)

        result = cb_env.collection.get(key)
        assert value == result.content_as[dict]


class ClassicSubDocumentTests(SubDocumentTestSuite):
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


class ProtostellarSubDocumentTests(SubDocumentTestSuite):
    SKIP_LIST = {
        'test_lookup_in_multiple_specs': 'ING-58',
        'test_lookup_in_one_path_not_found': 'ING-58',
        'test_lookup_in_simple_exists': 'ING-58',
        'test_lookup_in_simple_exists_bad_path': 'ING-58',
        'test_mutate_in_simple': 'ING-58',
        'test_mutate_in_simple_spec_as_list': 'ING-58',
    }

    @pytest.fixture(scope='function', autouse=True)
    def can_run_test(self, request):
        test_name = request.function.__name__
        if test_name in ProtostellarSubDocumentTests.SKIP_LIST:
            pytest.skip(ProtostellarSubDocumentTests.SKIP_LIST[test_name])


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