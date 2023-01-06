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

import couchbase.subdocument as SD
from couchbase.exceptions import (DocumentExistsException,
                                  DocumentNotFoundException,
                                  InvalidArgumentException,
                                  InvalidValueException,
                                  PathExistsException,
                                  PathMismatchException,
                                  PathNotFoundException)
from couchbase.options import GetOptions, MutateInOptions
from protostellar.result import (GetResult,
                              LookupInResult)

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class SubDocumentTests:
    NO_KEY = "not-a-key"

    @pytest.fixture(scope="class", name="cb_env", params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, couchbase_config, request):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, request.param, manage_buckets=True)
        # if request.param == CollectionType.NAMED:
        #     cb_env.try_n_times(5, 3, cb_env.setup_named_collections)

        cb_env.try_n_times(3, 5, cb_env.load_data)
        yield cb_env
        cb_env.try_n_times(3, 5, cb_env.purge_data)
        # if request.param == CollectionType.NAMED:
        #     cb_env.try_n_times_till_exception(5, 3,
        #                                       cb_env.teardown_named_collections,
        #                                       raise_if_no_exception=False)

    @pytest.fixture(name="new_kvp")
    def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest.fixture(name="default_kvp_and_reset")
    def default_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times(5, 3, cb_env.collection.upsert, key, value)

    def test_lookup_in_simple_get(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_default_key_value()
        result = cb.lookup_in(key, (SD.get("geo"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[dict](0) == value["geo"]

    def test_lookup_in_simple_get_spec_as_list(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = cb.lookup_in(key, [SD.get("geo")])
        assert isinstance(result, LookupInResult)
        assert result.content_as[dict](0) == value["geo"]

    def test_lookup_in_simple_exists(self, cb_env):
        pytest.skip('TBD')
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = cb.lookup_in(key, (SD.exists("geo"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        # no value content w/ EXISTS operation
        with pytest.raises(DocumentNotFoundException):
            result.content_as[bool](0)

    def test_lookup_in_simple_exists_bad_path(self, cb_env):
        pytest.skip('TBD')
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = cb.lookup_in(key, (SD.exists("qzzxy"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0) is False
        with pytest.raises(PathNotFoundException):
            result.content_as[bool](0)

    def test_lookup_in_one_path_not_found(self, cb_env):
        pytest.skip('TBD')
        cb = cb_env.collection
        key, _ = cb_env.get_default_key_value()
        result = cb.lookup_in(
            key, (SD.exists("geo"), SD.exists("qzzxy"),))
        assert isinstance(result, LookupInResult)
        assert result.exists(0)
        assert result.exists(1) is False
        with pytest.raises(DocumentNotFoundException):
            result.content_as[bool](0)
        with pytest.raises(PathNotFoundException):
            result.content_as[bool](1)

    def test_lookup_in_simple_long_path(self, cb_env):
        pytest.skip('TBD')
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        # add longer path to doc
        value["long_path"] = {"a": {"b": {"c": "yo!"}}}
        cb.upsert(key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = cb.lookup_in(
            key, (SD.get("long_path.a.b.c"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[str](0) == value["long_path"]["a"]["b"]["c"]
        # reset to norm
        cb.remove(key)

    def test_count(self, cb_env):
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        value["count"] = [1, 2, 3, 4, 5]
        cb.upsert(key, value)
        cb_env.try_n_times(10, 3, cb.get, key)
        result = cb.lookup_in(key, (SD.count("count"),))
        assert isinstance(result, LookupInResult)
        assert result.content_as[int](0) == 5

    def test_mutate_in_simple(self, cb_env):
        pytest.skip('TBD')
        cb = cb_env.collection
        key, value = cb_env.get_new_key_value()
        cb.upsert(key, value)
        cb_env.try_n_times(10, 3, cb.get, key)

        result = cb.mutate_in(key,
                              (SD.upsert("city", "New City"),
                               SD.replace("faa", "CTY")))

        value["city"] = "New City"
        value["faa"] = "CTY"

        def cas_matches(cb, new_cas):
            r = cb.get(key)
            if new_cas != r.cas:
                raise Exception(f"{new_cas} != {r.cas}")

        cb_env.try_n_times(10, 3, cas_matches, cb, result.cas)

        result = cb.get(key)
        assert value == result.content_as[dict]