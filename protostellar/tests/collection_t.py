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

import couchbase.subdocument as SD
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import (AmbiguousTimeoutException,
                                  CasMismatchException,
                                  DocumentExistsException,
                                  DocumentLockedException,
                                  DocumentNotFoundException,
                                  DocumentUnretrievableException,
                                  InvalidArgumentException,
                                  PathNotFoundException,
                                  TemporaryFailException)
from protostellar.options import (GetOptions,
                                  InsertOptions,
                                  ReplaceOptions,
                                  UpsertOptions)
from protostellar.result import (ExistsResult,
                                 GetResult,
                                 MutationResult)

from ._test_utils import (CollectionType,
                          KVPair,
                          TestEnvironment)


class CollectionTests:
    NO_KEY = "not-a-key"
    FIFTY_YEARS = 50 * 365 * 24 * 60 * 60
    THIRTY_DAYS = 30 * 24 * 60 * 60

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

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest.fixture(name="default_kvp_and_reset")
    def default_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times(5, 3, cb_env.collection.upsert, key, value)

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

    # def test_exists(self, cb_env, default_kvp):
    #     cb = cb_env.collection
    #     key = default_kvp.key
    #     result = cb.exists(key)
    #     assert isinstance(result, ExistsResult)
    #     assert result.exists is True

    def test_get(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = cb.get(key)
        assert isinstance(result, GetResult)
        assert result.cas is not None
        assert result.key == key
        assert result.expiry_time is None
        assert result.content_as[dict] == value

    def test_get_fails(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            cb.get(self.NO_KEY)

    def test_upsert(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = cb.upsert(key, value, UpsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_insert(self, cb_env, new_kvp):
        cb = cb_env.collection
        key = new_kvp.key
        value = new_kvp.value
        result = cb.insert(key, value, InsertOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_insert_document_exists(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        with pytest.raises(DocumentExistsException):
            cb.insert(key, value)

    def test_replace(self, cb_env, default_kvp):
        cb = cb_env.collection
        key = default_kvp.key
        value = default_kvp.value
        result = cb.replace(key, value, ReplaceOptions(
            timeout=timedelta(seconds=3)))
        assert result is not None
        assert isinstance(result, MutationResult)
        assert result.cas != 0
        g_result = cb_env.try_n_times(10, 3, cb.get, key)
        assert g_result.key == key
        assert value == g_result.content_as[dict]

    def test_replace_with_cas(self, cb_env, default_kvp_and_reset, new_kvp):
        pytest.skip('TBD')
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        value1 = new_kvp.value
        result = cb.get(key)
        old_cas = result.cas
        result = cb.replace(key, value1, ReplaceOptions(cas=old_cas))
        assert isinstance(result, MutationResult)
        assert result.cas != old_cas

        # try same cas again, must fail.
        with pytest.raises(CasMismatchException):
            cb.replace(key, value1, ReplaceOptions(cas=old_cas))

    def test_replace_fail(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            cb.replace(self.NO_KEY, {"some": "content"})

    def test_remove(self, cb_env, default_kvp_and_reset):
        cb = cb_env.collection
        key = default_kvp_and_reset.key
        result = cb.remove(key)
        assert isinstance(result, MutationResult)

        with pytest.raises(DocumentNotFoundException):
            cb_env.try_n_times_till_exception(3,
                                              1,
                                              cb.get,
                                              key,
                                              expected_exceptions=(DocumentNotFoundException,),
                                              raise_exception=True)

    def test_remove_fail(self, cb_env):
        cb = cb_env.collection
        with pytest.raises(DocumentNotFoundException):
            cb.remove(self.NO_KEY)
