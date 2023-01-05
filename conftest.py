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

pytest_plugins = [
    "tests.helpers"
]

_DIAGNOSTIC_TESTS = [
    "acouchbase/tests/bucket_t.py::BucketDiagnosticsTests",
    "acouchbase/tests/cluster_t.py::ClusterDiagnosticsTests",
    "couchbase/tests/bucket_t.py::BucketDiagnosticsTests",
    "couchbase/tests/cluster_t.py::ClusterDiagnosticsTests",
]

_KV_TESTS = [
    "acouchbase/tests/collection_t.py::CollectionTests",
    "acouchbase/tests/subdoc_t.py::SubDocumentTests",
    "acouchbase/tests/mutation_tokens_t.py::MutationTokensEnabledTests",
    "acouchbase/tests/binary_collection_t.py::BinaryCollectionTests",
    "acouchbase/tests/datastructures_t.py::DatastructuresTests",
    "acouchbase/tests/transcoder_t.py::DefaultTranscoderTests",
    "couchbase/tests/collection_t.py::CollectionTests",
    "couchbase/tests/collection_multi_t.py::CollectionMultiTests",
    "couchbase/tests/subdoc_t.py::SubDocumentTests",
    "couchbase/tests/mutation_tokens_t.py::MutationTokensEnabledTests",
    "couchbase/tests/binary_collection_t.py::BinaryCollectionTests",
    "couchbase/tests/binary_collection_multi_t.py::BinaryCollectionMultiTests",
    "couchbase/tests/datastructures_t.py::DatastructuresTests",
    "couchbase/tests/datastructures_t.py::LegacyDatastructuresTests",
    "couchbase/tests/transcoder_t.py::DefaultTranscoderTests",
    "txcouchbase/tests/collection_t.py::CollectionTests",
    "txcouchbase/tests/subdoc_t.py::SubDocumentTests",
    "txcouchbase/tests/mutation_tokens_t.py::MutationTokensEnabledTests",
    "txcouchbase/tests/binary_collection_t.py::BinaryCollectionTests",
    "txcouchbase/tests/transcoder_t.py::DefaultTranscoderTests",
    "protostellar/tests/collection_t.py::CollectionTests",
]

_STREAMING_TESTS = [
    "acouchbase/tests/query_t.py::QueryTests",
    "acouchbase/tests/query_t.py::QueryCollectionTests",
    "acouchbase/tests/analytics_t.py::AnalyticsTests",
    "acouchbase/tests/analytics_t.py::AnalyticsCollectionTests",
    "acouchbase/tests/search_t.py::SearchTests",
    "acouchbase/tests/search_t.py::SearchCollectionTests",
    "acouchbase/tests/views_t.py::ViewTests",
    "couchbase/tests/query_t.py::QueryTests",
    "couchbase/tests/query_t.py::QueryCollectionTests",
    "couchbase/tests/query_t.py::QueryParamTests",
    "couchbase/tests/analytics_t.py::AnalyticsTests",
    "couchbase/tests/analytics_t.py::AnalyticsCollectionTests",
    "couchbase/tests/search_t.py::SearchTests",
    "couchbase/tests/search_t.py::SearchCollectionTests",
    "couchbase/tests/search_t.py::SearchStringTests",
    "couchbase/tests/views_t.py::ViewTests",
]

_MGMT_TESTS = [
    "acouchbase/tests/analyticsmgmt_t.py::AnalyticsManagementTests",
    "acouchbase/tests/analyticsmgmt_t.py::AnalyticsManagementLinksTests",
    "acouchbase/tests/bucketmgmt_t.py::BucketManagementTests",
    "acouchbase/tests/collectionmgmt_t.py::CollectionManagementTests",
    "acouchbase/tests/eventingmgmt_t.py::EventingManagementTests",
    "acouchbase/tests/querymgmt_t.py::QueryIndexManagementTests",
    "acouchbase/tests/querymgmt_t.py::QueryIndexCollectionManagementTests",
    "acouchbase/tests/searchmgmt_t.py::SearchIndexManagementTests",
    "acouchbase/tests/usermgmt_t.py::UserManagementTests",
    "acouchbase/tests/viewmgmt_t.py::ViewIndexManagementTests",
    "couchbase/tests/analyticsmgmt_t.py::AnalyticsManagementTests",
    "couchbase/tests/analyticsmgmt_t.py::AnalyticsManagementLinksTests",
    "couchbase/tests/bucketmgmt_t.py::BucketManagementTests",
    "couchbase/tests/collectionmgmt_t.py::CollectionManagementTests",
    "couchbase/tests/eventingmgmt_t.py::EventingManagementTests",
    "couchbase/tests/querymgmt_t.py::QueryIndexManagementTests",
    "couchbase/tests/querymgmt_t.py::QueryIndexCollectionManagementTests",
    "couchbase/tests/searchmgmt_t.py::SearchIndexManagementTests",
    "couchbase/tests/usermgmt_t.py::UserManagementTests",
    "couchbase/tests/viewmgmt_t.py::ViewIndexManagementTests"
]

_SLOW_MGMT_TESTS = [
    "acouchbase/tests/eventingmgmt_t.py::EventingManagementTests",
    "couchbase/tests/eventingmgmt_t.py::EventingManagementTests",
]

_MISC_TESTS = [
    "acouchbase/tests/rate_limit_t.py::RateLimitTests",
    "couchbase/tests/connection_t.py::ConnectionTests"
    "couchbase/tests/rate_limit_t.py::RateLimitTests",
]

_TXNS_TESTS = [
    "acouchbase/tests/transactions_t.py::AsyncTransactionsTests",
    "couchbase/tests/transactions_t.py::TransactionTests",
]


@pytest.fixture(name="couchbase_config", scope="session")
def get_config(couchbase_test_config):
    if couchbase_test_config.mock_server_enabled:
        print("Mock server enabled!")
    if couchbase_test_config.real_server_enabled:
        print("Real server enabled!")

    return couchbase_test_config


def pytest_addoption(parser):
    parser.addoption(
        "--txcouchbase", action="store_true", default=False, help="run txcouchbase tests"
    )


def pytest_collection_modifyitems(items):  # noqa: C901
    for item in items:
        item_details = item.nodeid.split('::')

        item_api = item_details[0].split('/')
        if item_api[0] == 'couchbase':
            item.add_marker(pytest.mark.pycbc_couchbase)
        elif item_api[0] == 'acouchbase':
            item.add_marker(pytest.mark.pycbc_acouchbase)
        elif item_api[0] == 'txcouchbase':
            item.add_marker(pytest.mark.pycbc_txcouchbase)

        test_class_path = '::'.join(item_details[:-1])
        if test_class_path in _DIAGNOSTIC_TESTS:
            item.add_marker(pytest.mark.pycbc_diag)
        elif test_class_path in _KV_TESTS:
            item.add_marker(pytest.mark.pycbc_kv)
        elif test_class_path in _STREAMING_TESTS:
            item.add_marker(pytest.mark.pycbc_streaming)
        elif test_class_path in _MGMT_TESTS:
            item.add_marker(pytest.mark.pycbc_mgmt)
        elif test_class_path in _MISC_TESTS:
            item.add_marker(pytest.mark.pycbc_misc)
        elif test_class_path in _TXNS_TESTS:
            item.add_marker(pytest.mark.pycbc_txn)

        if test_class_path in _SLOW_MGMT_TESTS:
            item.add_marker(pytest.mark.pycbc_slow_mgmt)
