#  Copyright 2016-2023. Couchbase, Inc.
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

from new_couchbase.api import ApiImplementation
from new_couchbase.durability import (ClientDurability,
                                  DurabilityLevel,
                                  PersistTo,
                                  PersistToExtended,
                                  ReplicateTo,
                                  ServerDurability)
from new_couchbase.exceptions import DurabilityImpossibleException, FeatureUnavailableException
from new_couchbase.options import (AppendOptions,
                               DecrementOptions,
                               IncrementOptions,
                               PrependOptions)
from new_couchbase.transcoder import RawStringTranscoder
from tests.environments import CollectionType
from tests.environments.binary_environment import BinaryTestEnvironment
from tests.environments.test_environment import TestEnvironment
from tests.test_features import EnvironmentFeatures


class BinaryDurabilityTestSuite:

    TEST_MANIFEST = [
        'test_client_durable_append',
        'test_client_durable_append_fail',
        'test_client_durable_append_single_node',
        'test_client_durable_decrement',
        'test_client_durable_decrement_fail',
        'test_client_durable_decrement_single_node',
        'test_client_durable_increment',
        'test_client_durable_increment_fail',
        'test_client_durable_increment_single_node',
        'test_client_durable_prepend',
        'test_client_durable_prepend_fail',
        'test_client_durable_prepend_single_node',
        'test_server_durable_append',
        'test_server_durable_append_single_node',
        'test_server_durable_decrement',
        'test_server_durable_decrement_single_node',
        'test_server_durable_increment',
        'test_server_durable_increment_single_node',
        'test_server_durable_prepend',
        'test_server_durable_prepend_single_node',
    ]

    @pytest.fixture(scope='class')
    def check_has_replicas(self, num_replicas):
        if num_replicas == 0:
            pytest.skip('No replicas to test durability.')

    @pytest.fixture(scope='class')
    def check_multi_node(self, num_nodes):
        if num_nodes == 1:
            pytest.skip('Test only for clusters with more than a single node.')

    @pytest.fixture(scope='class')
    def check_single_node(self, num_nodes):
        if num_nodes != 1:
            pytest.skip('Test only for clusters with a single node.')

    @pytest.fixture(scope='class')
    def check_sync_durability_supported(self, cb_env):
        EnvironmentFeatures.check_if_feature_supported('sync_durability',
                                                       cb_env.server_version_short,
                                                       cb_env.mock_server_type)

    @pytest.fixture(scope='class')
    def num_replicas(self, cb_env):
        # @TODO(jc): remove once bucket mgmt is available
        if cb_env.cluster.api_implementation == ApiImplementation.PROTOSTELLAR:
            return 1
        bucket_settings = TestEnvironment.try_n_times(10, 1, cb_env.bm.get_bucket, cb_env.bucket.name)
        num_replicas = bucket_settings.get('num_replicas')
        return num_replicas

    @pytest.fixture(scope='class')
    def num_nodes(self, cb_env):
        # @TODO(jc): remove once bucket mgmt is available
        if cb_env.cluster.api_implementation == ApiImplementation.PROTOSTELLAR:
            return 1
        return len(cb_env.cluster.cluster_info(refresh=False).nodes)

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_append(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))
        result = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_append_fail(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_append_single_node(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_decrement(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc_by_type('counter')
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        result = cb_env.collection.binary().decrement(key, DecrementOptions(durability=durability))
        assert result.content == value - 1

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_decrement_fail(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().decrement(key, DecrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_decrement_single_node(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().decrement(key, DecrementOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_increment(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc_by_type('counter')
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        result = cb_env.collection.binary().increment(key, IncrementOptions(durability=durability))
        assert result.content == value + 1

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_increment_fail(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_increment_single_node(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_prepend(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        cb_env.collection.binary().prepend(key, 'foo',  PrependOptions(durability=durability))
        result = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_prepend_fail(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().prepend(key, 'foo',  PrependOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_prepend_single_node(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().prepend(key, 'foo',  PrependOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_append(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))
        result = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_append_single_node(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_decrement(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('counter')
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        result = cb_env.collection.binary().decrement(key, DecrementOptions(durability=durability))
        assert result.content == value - 1

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_decrement_single_node(self, cb_env):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().decrement(key, DecrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_increment(self, cb_env):
        key, value = cb_env.get_existing_doc_by_type('counter')
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        result = cb_env.collection.binary().increment(key, IncrementOptions(durability=durability))
        assert result.content == value + 1

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_increment_single_node(self, cb_env):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_prepend(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))
        result = cb_env.collection.get(key, transcoder=RawStringTranscoder())
        assert result.content_as[str] == 'foo'

    @pytest.mark.usefixtures("check_sync_durability_supported")
    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_server_durable_prepend_single_node(self, cb_env):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
        with pytest.raises(DurabilityImpossibleException):
            cb_env.collection.binary().prepend(key, 'foo',  PrependOptions(durability=durability))


class ClassicBinaryDurabilityTests(BinaryDurabilityTestSuite):

    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ClassicBinaryDurabilityTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ClassicBinaryDurabilityTests) if valid_test_method(meth)]
        test_list = set(BinaryDurabilityTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    # @TODO(jc):  Add CollectionType.NAMED
    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, request):
        cb_env = BinaryTestEnvironment.from_environment(cb_base_env)
        cb_env.enable_bucket_mgmt()
        cb_env.setup(request.param, __name__)
        yield cb_env
        cb_env.teardown(request.param)

class ProtostellarBinaryDurabilityTests(BinaryDurabilityTestSuite):

    SKIP_LIST = {
        # 'test_client_durable_append': '',
        # 'test_client_durable_append_fail': '',
        # 'test_client_durable_append_single_node': '',
        # 'test_client_durable_decrement': '',
        # 'test_client_durable_decrement_fail': '',
        # 'test_client_durable_decrement_single_node': '',
        # 'test_client_durable_increment': '',
        # 'test_client_durable_increment_fail': '',
        # 'test_client_durable_increment_single_node': '',
        # 'test_client_durable_prepend': '',
        # 'test_client_durable_prepend_fail': '',
        # 'test_client_durable_prepend_single_node': '',
        # 'test_server_durable_append': '',
        # 'test_server_durable_append_single_node': '',
        # 'test_server_durable_decrement': '',
        # 'test_server_durable_decrement_single_node': '',
        # 'test_server_durable_increment': '',
        # 'test_server_durable_increment_single_node': '',
        # 'test_server_durable_prepend': '',
        # 'test_server_durable_prepend_single_node': '',
    }

    @pytest.fixture(scope='function', autouse=True)
    def can_run_test(self, request):
        test_name = request.function.__name__
        if test_name in ProtostellarBinaryDurabilityTests.SKIP_LIST:
            reason = ProtostellarBinaryDurabilityTests.SKIP_LIST[test_name]
            pytest.skip(f'Skipping {test_name} due to : {reason}')


    @pytest.fixture(scope='class')
    def test_manifest_validated(self):
        def valid_test_method(meth):
            attr = getattr(ProtostellarBinaryDurabilityTests, meth)
            return callable(attr) and not meth.startswith('__') and meth.startswith('test')
        method_list = [meth for meth in dir(ProtostellarBinaryDurabilityTests) if valid_test_method(meth)]
        test_list = set(BinaryDurabilityTestSuite.TEST_MANIFEST).symmetric_difference(method_list)
        if test_list:
            pytest.fail(f'Test manifest invalid.  Missing/extra tests: {test_list}.')

    # @TODO(jc):  Add CollectionType.NAMED
    @pytest.fixture(scope='class', name='cb_env', params=[CollectionType.DEFAULT])
    def couchbase_test_environment(self, cb_base_env, request):
        cb_env = BinaryTestEnvironment.from_environment(cb_base_env)
        # @TODO(jc):  Add once bucket mgmt available
        # cb_base_env.enable_bucket_mgmt()
        cb_env.setup(request.param, __name__)
        yield cb_env
        cb_env.teardown(request.param)

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_append(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_append_fail(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_append_single_node(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().append(key, 'foo',  AppendOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_decrement(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc_by_type('counter')
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            result = cb_env.collection.binary().decrement(key, DecrementOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_decrement_fail(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().decrement(key, DecrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_decrement_single_node(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().decrement(key, DecrementOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_increment(self, cb_env, num_replicas):
        key, value = cb_env.get_existing_doc_by_type('counter')
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            result = cb_env.collection.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_increment_fail(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_increment_single_node(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('counter', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().increment(key, IncrementOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_prepend(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistTo.ONE, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().prepend(key, 'foo',  PrependOptions(durability=durability))

    @pytest.mark.usefixtures('check_multi_node')
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_prepend_fail(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().prepend(key, 'foo',  PrependOptions(durability=durability))

    @pytest.mark.usefixtures("check_single_node")
    @pytest.mark.usefixtures('check_has_replicas')
    def test_client_durable_prepend_single_node(self, cb_env, num_replicas):
        key = cb_env.get_existing_doc_by_type('utf8_empty', key_only=True)
        durability = ClientDurability(
            persist_to=PersistToExtended.FOUR, replicate_to=ReplicateTo(num_replicas))
        with pytest.raises(FeatureUnavailableException):
            cb_env.collection.binary().prepend(key, 'foo',  PrependOptions(durability=durability))
