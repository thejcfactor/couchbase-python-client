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

from __future__ import annotations

import json
import os
import pathlib
import random
import time
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import (Any,
                    Callable,
                    Dict,
                    List,
                    Optional,
                    Union,
                    Tuple,
                    Type,
                    TYPE_CHECKING)
from urllib.parse import urlparse

import pytest

from new_couchbase.api import ApiImplementation
from new_couchbase.auth import PasswordAuthenticator
from new_couchbase.cluster import Cluster
from new_couchbase.exceptions import (AmbiguousTimeoutException, 
                                      CouchbaseException,
                                      FeatureUnavailableException,
                                      UnAmbiguousTimeoutException)
from new_couchbase.options import ClusterOptions

if TYPE_CHECKING:
    from .mock_server import (MockServer,
                              MockServerType)


class CouchbaseTestEnvironmentException(Exception):
    """Raised when something with the test environment is incorrect."""

class CollectionType(IntEnum):
    DEFAULT = 1
    NAMED = 2

KVPair = namedtuple("KVPair", "key value")

@dataclass
class RateLimitData:
    url: str = None
    username: str = None
    pw: str = None
    fts_indexes: List[str] = None

class ServerFeatures(Enum):
    KeyValue = 'kv'
    SSL = 'ssl'
    Views = 'views'
    SpatialViews = 'spatial_views'
    Diagnostics = 'diagnostics'
    SynchronousDurability = 'sync_durability'
    Query = 'query'
    Subdoc = 'subdoc'
    Xattr = 'xattr'
    Search = 'search'
    Analytics = 'analytics'
    Collections = 'collections'
    Replicas = 'replicas'
    UserManagement = 'user_mgmt'
    BasicBucketManagement = 'basic_bucket_mgmt'
    BucketManagement = 'bucket_mgmt'
    BucketMinDurability = 'bucket_min_durability'
    BucketStorageBackend = 'bucket_storage_backend'
    CustomConflictResolution = 'custom_conflict_resolution'
    QueryIndexManagement = 'query_index_mgmt'
    SearchIndexManagement = 'search_index_mgmt'
    ViewIndexManagement = 'view_index_mgmt'
    GetMeta = 'get_meta'
    AnalyticsPendingMutations = 'analytics_pending_mutations'
    AnalyticsLinkManagement = 'analytics_link_mgmt'
    UserGroupManagement = 'user_group_mgmt'
    PreserveExpiry = 'preserve_expiry'
    SearchDisableScoring = 'search_disable_scoring'
    Eventing = 'eventing'
    EventingFunctionManagement = 'eventing_function_mgmt'
    RateLimiting = 'rate_limiting'
    Txns = 'txns'
    TxnQueries = 'txn_queries'

class EnvironmentFeatures:
    # mock and real server (all versions) should have these features
    BASIC_FEATURES = [ServerFeatures.KeyValue,
                    ServerFeatures.Diagnostics,
                    ServerFeatures.SSL,
                    ServerFeatures.SpatialViews,
                    ServerFeatures.Subdoc,
                    ServerFeatures.Views,
                    ServerFeatures.Replicas]

    # mock related feature lists
    FEATURES_NOT_IN_MOCK = [ServerFeatures.Analytics,
                            ServerFeatures.BucketManagement,
                            ServerFeatures.EventingFunctionManagement,
                            ServerFeatures.GetMeta,
                            ServerFeatures.Query,
                            ServerFeatures.QueryIndexManagement,
                            ServerFeatures.RateLimiting,
                            ServerFeatures.Search,
                            ServerFeatures.SearchIndexManagement,
                            ServerFeatures.TxnQueries,
                            ServerFeatures.UserGroupManagement,
                            ServerFeatures.UserManagement,
                            ServerFeatures.ViewIndexManagement]

    FEATURES_IN_MOCK = [ServerFeatures.Txns]

    # separate features into CBS versions, lets make 5.5 the earliest
    AT_LEAST_V5_5_0_FEATURES = [ServerFeatures.BucketManagement,
                                ServerFeatures.GetMeta,
                                ServerFeatures.Query,
                                ServerFeatures.QueryIndexManagement,
                                ServerFeatures.Search,
                                ServerFeatures.SearchIndexManagement,
                                ServerFeatures.ViewIndexManagement]

    AT_LEAST_V6_0_0_FEATURES = [ServerFeatures.Analytics,
                                ServerFeatures.UserManagement]

    AT_LEAST_V6_5_0_FEATURES = [ServerFeatures.AnalyticsPendingMutations,
                                ServerFeatures.UserGroupManagement,
                                ServerFeatures.SynchronousDurability,
                                ServerFeatures.SearchDisableScoring]

    AT_LEAST_V6_6_0_FEATURES = [ServerFeatures.BucketMinDurability,
                                ServerFeatures.Txns]

    AT_LEAST_V7_0_0_FEATURES = [ServerFeatures.Collections,
                                ServerFeatures.AnalyticsLinkManagement,
                                ServerFeatures.TxnQueries]

    AT_LEAST_V7_1_0_FEATURES = [ServerFeatures.RateLimiting,
                                ServerFeatures.BucketStorageBackend,
                                ServerFeatures.CustomConflictResolution,
                                ServerFeatures.EventingFunctionManagement,
                                ServerFeatures.PreserveExpiry]

    # Only set the baseline needed
    TEST_SUITE_MAP = {
        'analytics_t': [ServerFeatures.Analytics],
        'analyticsmgmt_t': [ServerFeatures.Analytics],
        'binary_collection_multi_t': [ServerFeatures.KeyValue],
        'binary_collection_t': [ServerFeatures.KeyValue],
        'binary_durability_t': [ServerFeatures.KeyValue],
        'bucket_t': [ServerFeatures.Diagnostics],
        'bucketmgmt_t': [ServerFeatures.BucketManagement],
        'cluster_t': [ServerFeatures.Diagnostics],
        'collection_multi_t': [ServerFeatures.KeyValue],
        'collection_t': [ServerFeatures.KeyValue],
        'collectionmgmt_t': [ServerFeatures.Collections],
        'connection_t': [ServerFeatures.Diagnostics],
        'datastructures_t': [ServerFeatures.Subdoc],
        'durability_t': [ServerFeatures.KeyValue],
        'eventingmgmt_t': [ServerFeatures.EventingFunctionManagement],
        'metrics_t': [ServerFeatures.Collections],
        'mutation_tokens_t': [ServerFeatures.KeyValue],
        'query_t': [ServerFeatures.Query, ServerFeatures.QueryIndexManagement],
        'querymgmt_t': [ServerFeatures.QueryIndexManagement],
        'rate_limit_t': [ServerFeatures.RateLimiting,
                        ServerFeatures.BucketManagement,
                        ServerFeatures.UserManagement,
                        ServerFeatures.Collections],
        'search_t': [ServerFeatures.Search, ServerFeatures.SearchIndexManagement],
        'searchmgmt_t': [ServerFeatures.SearchIndexManagement],
        'subdoc_t': [ServerFeatures.Subdoc],
        'tracing_t': [ServerFeatures.Collections],
        'transactions_t': [ServerFeatures.Txns],
        'transcoder_t': [ServerFeatures.KeyValue],
        'usermgmt_t': [ServerFeatures.UserManagement],
        'viewmgmt_t': [ServerFeatures.ViewIndexManagement],
        'views_t': [ServerFeatures.Views, ServerFeatures.ViewIndexManagement]
    }

    @staticmethod
    def is_feature_supported(feature,  # type: str
                              server_version, # type: float
                              mock_server_type=None # type: Optional[MockServerType]
                             ) -> bool:
        try:
            supported = EnvironmentFeatures.supports_feature(feature, server_version, mock_server_type)
            return supported
        except Exception:
            return False

    @staticmethod
    def check_if_feature_supported(features,  # type: Union[str, List[str]]
                                   server_version, # type: float
                                   mock_server_type=None # type: Optional[MockServerType]
                                   ) -> None:

        features_list = []
        if isinstance(features, str):
            features_list.append(features)
        else:
            features_list.extend(features)

        for feature in features_list:
            try:
                supported = EnvironmentFeatures.supports_feature(feature, server_version, mock_server_type)
                if not supported:
                    pytest.skip(EnvironmentFeatures.feature_not_supported_text(feature))
            except TypeError:
                pytest.skip("Unable to determine server version")
            except Exception:
                raise

    @staticmethod
    def supports_feature(feature,  # type: str  # noqa: C901
                         server_version, # type: float
                         mock_server_type=None # type: Optional[MockServerType]
                         ) -> bool:

        is_mock_server = mock_server_type is not None
        is_real_server = is_mock_server is False

        if feature in map(lambda f: f.value, EnvironmentFeatures.BASIC_FEATURES):
            return True

        if is_mock_server and feature in map(lambda f: f.value, EnvironmentFeatures.FEATURES_NOT_IN_MOCK):
            return False

        if is_mock_server and feature in map(lambda f: f.value, EnvironmentFeatures.FEATURES_IN_MOCK):
            return True

        if feature == ServerFeatures.Diagnostics.value:
            if is_real_server:
                return True

            return mock_server_type == MockServerType.GoCAVES

        if feature == ServerFeatures.Xattr.value:
            if is_real_server:
                return True

            return mock_server_type == MockServerType.GoCAVES

        if feature == ServerFeatures.BasicBucketManagement.value:
            if is_real_server:
                return True

            return mock_server_type == MockServerType.GoCAVES

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V5_5_0_FEATURES):
            if is_real_server:
                return server_version >= 5.5
            return not is_mock_server

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_0_0_FEATURES):
            if is_real_server:
                return server_version >= 6.0
            # @TODO: couchbase++ looks to choke w/ CAVES
            # if feature == ServerFeatures.UserManagement.value:
            #     return self.mock_server_type == MockServerType.GoCAVES
            return not is_mock_server

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_5_0_FEATURES):
            if is_real_server:
                return server_version >= 6.5
            return not is_mock_server

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_6_0_FEATURES):
            if is_real_server:
                return server_version >= 6.6
            return not is_mock_server

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V7_0_0_FEATURES):
            if is_real_server:
                return server_version >= 7.0
            if feature == ServerFeatures.Collections.value:
                return mock_server_type == MockServerType.GoCAVES
            return not is_mock_server

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V7_1_0_FEATURES):
            if is_real_server:
                return server_version >= 7.1
            return not is_mock_server

        raise CouchbaseTestEnvironmentException(f"Unable to determine if server has provided feature: {feature}")

    @staticmethod
    def feature_not_supported_text(feature,  # type: str  # noqa: C901
                                   server_version, # type: float
                                   mock_server_type=None # type: Optional[MockServerType]
                                   ) -> str:

        is_mock_server = mock_server_type is not None
        is_real_server = is_mock_server is False

        if is_mock_server and feature in map(lambda f: f.value, EnvironmentFeatures.FEATURES_NOT_IN_MOCK):
            return f'Mock server does not support feature: {feature}'

        if feature == ServerFeatures.Diagnostics.value:
            if mock_server_type == MockServerType.Legacy:
                return f'LegacyMockServer does not support feature: {feature}'

        if feature == ServerFeatures.Xattr.value:
            if mock_server_type == MockServerType.Legacy:
                return f'LegacyMockServer does not support feature: {feature}'

        if feature == ServerFeatures.BucketManagement.value:
            if mock_server_type == MockServerType.Legacy:
                return f'LegacyMockServer does not support feature: {feature}'

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V5_5_0_FEATURES):
            if is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 5.5. '
                        f'Using server version: {server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_0_0_FEATURES):
            if is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 6.0. '
                        f'Using server version: {server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_5_0_FEATURES):
            if is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 6.5. '
                        f'Using server version: {server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V6_6_0_FEATURES):
            if is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 6.6. '
                        f'Using server version: {server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V7_0_0_FEATURES):
            if is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 7.0. '
                        f'Using server version: {server_version}.')
            return f'Mock server does not support feature: {feature}'

        if feature in map(lambda f: f.value, EnvironmentFeatures.AT_LEAST_V7_1_0_FEATURES):
            if is_real_server:
                return (f'Feature: {feature} only supported on server versions >= 7.1. '
                        f'Using server version: {server_version}.')
            return f'Mock server does not support feature: {feature}'

    @staticmethod
    def mock_supports_feature(test_suite,  # type: str
                              is_mock_server,  # type: bool
                              is_protostellar=False,  # type: bool
                              ) -> bool:
        if not is_mock_server:
            return True

        if is_protostellar:
            return False

        test_suite_features = EnvironmentFeatures.TEST_SUITE_MAP.get(test_suite, None)
        if not test_suite_features:
            raise CouchbaseTestEnvironmentException(f"Unable to determine features for test suite: {test_suite}")

        for feature in test_suite_features:
            if feature.value in map(lambda f: f.value, EnvironmentFeatures.FEATURES_NOT_IN_MOCK):
                return False

        return True



class TestEnvironment:
    NOT_A_KEY = 'not-a-key'
    TEST_SCOPE = "test-scope"
    TEST_COLLECTION = "test-collection"
    
    def __init__(self, 
                **kwargs # type: Dict[str, Any]
                ):
        
        self._bucket = kwargs.pop('bucket', None)
        self._cluster = kwargs.pop('cluster', None)
        self._collection = kwargs.pop('collection', None)
        self._config = kwargs.pop('couchbase_config', None)
        self._extra_docs = {}
        self._loaded_docs = {}
        self._named_collection = None
        self._named_scope = None
        self._scope = self._collection._scope if self._collection else None
        self._test_bucket = None
        self._test_bucket_cm = None
        self._used_docs = set()
        self._used_extras = set()

        if kwargs.get("manage_buckets", False) is True:
            EnvironmentFeatures.check_if_feature_supported('basic_bucket_mgmt',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            # self._bm = self.cluster.buckets()

        if kwargs.get("manage_collections", False) is True:
            EnvironmentFeatures.check_if_feature_supported('collections',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            # self._cm = self.bucket.collections()

        if kwargs.get("manage_users", False) is True:
            EnvironmentFeatures.check_if_feature_supported('user_mgmt',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            # self._um = self.cluster.users()

        if kwargs.get("manage_analytics", False) is True:
            EnvironmentFeatures.check_if_feature_supported('analytics',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            # self._am = self.cluster.analytics_indexes()

        if kwargs.get("manage_query_indexes", False) is True:
            EnvironmentFeatures.check_if_feature_supported('query_index_mgmt',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            # self._ixm = self.cluster.query_indexes()

        if kwargs.get("manage_search_indexes", False) is True:
            EnvironmentFeatures.check_if_feature_supported('search_index_mgmt',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            # self._sixm = self.cluster.search_indexes()

        if kwargs.get("manage_view_indexes", False) is True:
            EnvironmentFeatures.check_if_feature_supported('view_index_mgmt',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            # self._vixm = self.bucket.view_indexes()

        if kwargs.get('manage_eventing_functions', False) is True:
            EnvironmentFeatures.check_if_feature_supported('eventing_function_mgmt',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            # self._efm = self.cluster.eventing_functions()

        if kwargs.get('manage_rate_limit', False) is True:
            EnvironmentFeatures.check_if_feature_supported('rate_limiting',
                                                           self.server_version_short,
                                                           self.mock_server_type)
            parsed_conn = urlparse(self._config.get_connection_string())
            url = f'http://{parsed_conn.netloc}:8091'
            u, p = self._config.get_username_and_pw()
            self._rate_limit_params = RateLimitData(url, u, p)
        
    @property
    def am(self) -> Optional[Any]:
        """Returns the default AnalyticsIndexManager"""
        return self._am if hasattr(self, '_am') else None

    @property
    def bm(self) -> Optional[Any]:
        """Returns the default bucket's BucketManager"""
        return self._bm if hasattr(self, '_bm') else None

    @property
    def bucket(self):
        return self._bucket

    @property
    def cluster(self):
        return self._cluster

    @property
    def cm(self) -> Optional[Any]:
        """Returns the default CollectionManager"""
        return self._cm if hasattr(self, '_cm') else None

    @property
    def collection(self):
        if self._named_collection is None:
            return self._collection
        return self._named_collection

    @property
    def efm(self) -> Optional[Any]:
        """Returns the default EventingFunctionManager"""
        return self._efm if hasattr(self, '_efm') else None

    @property
    def fqdn(self) -> Optional[str]:
        return f'`{self.bucket.name}`.`{self.scope.name}`.`{self.collection.name}`'

    @property
    def is_developer_preview(self) -> Optional[bool]:
        return self._cluster.is_developer_preview

    @property
    def is_mock_server(self) -> MockServer:
        return self._config.is_mock_server

    @property
    def is_real_server(self):
        return not self._config.is_mock_server

    @property
    def ixm(self) -> Optional[Any]:
        """Returns the default QueryIndexManager"""
        return self._ixm if hasattr(self, '_ixm') else None

    # @property
    # def loaded_keys(self):
    #     return self._loaded_keys

    @property
    def mock_server_type(self) -> MockServerType:
        if self.is_mock_server:
            return self._config.mock_server.mock_type
        return None

    @property
    def rate_limit_params(self) -> Optional[RateLimitData]:
        """Returns the rate limit testing data"""
        return self._rate_limit_params if hasattr(self, '_rate_limit_params') else None

    @property
    def scope(self):
        if self._named_scope is None:
            return self._scope
        return self._named_scope

    @property
    def server_version(self) -> Optional[str]:
        return self._cluster.server_version

    @property
    def server_version_full(self) -> Optional[str]:
        return self._cluster.server_version_full

    @property
    def server_version_short(self) -> Optional[float]:
        return self._cluster.server_version_short

    @property
    def sixm(self) -> Optional[Any]:
        """Returns the default SearchIndexManager"""
        return self._sixm if hasattr(self, '_sixm') else None

    @property
    def test_bucket(self) -> Optional[Any]:
        """Returns the test bucket object"""
        return self._test_bucket if hasattr(self, '_test_bucket') else None

    @property
    def test_bucket_cm(self) -> Optional[Any]:
        """Returns the test bucket's CollectionManager"""
        return self._test_bucket_cm if hasattr(self, '_test_bucket_cm') else None

    @property
    def um(self) -> Optional[Any]:
        """Returns the default UserManager"""
        return self._um if hasattr(self, '_um') else None

    @property
    def vixm(self) -> Optional[Any]:
        """Returns the default ViewIndexManager"""
        return self._vixm if hasattr(self, '_vixm') else None

    def get_data_from_file(self):
        # TODO:  config # of documents loaded and set default key/doc
        data_types = ["airports", "airlines", "routes", "hotels", "landmarks"]
        sample_json = []
        with open(os.path.join(pathlib.Path(__file__).parent, "travel_sample_data.json")) as data_file:
            sample_data = data_file.read()
            sample_json = json.loads(sample_data)

        return data_types, sample_json

    def get_new_doc(self, has_field=None):
        if not self._extra_docs:
            self.load_extra_docs_from_file()

        all_keys = set(self._extra_docs.keys())
        available_keys = all_keys.difference(self._used_extras)
        if has_field:
            for _ in range(20):
                key = random.choice(list(available_keys))
                if has_field in self._extra_docs[key]:
                    break
                key = None
            
            if key is None:
                raise CouchbaseTestEnvironmentException(f'Unable to find doc w/ field: {has_field}')
        else:
            key = random.choice(list(available_keys))
        self._used_extras.add(key)
        return key, self._extra_docs[key]

    def get_existing_doc(self, has_field=None):
        if not self._loaded_docs:
            self.load_data()

        all_keys = set(self._loaded_docs.keys())
        available_keys = all_keys.difference(self._used_docs)
        if has_field:
            for _ in range(20):
                key = random.choice(list(available_keys))
                if has_field in self._loaded_docs[key]:
                    break
                key = None
            
            if key is None:
                raise CouchbaseTestEnvironmentException(f'Unable to find doc w/ field: {has_field}')
        else:
            key = random.choice(list(available_keys))
        self._used_docs.add(key)
        return key, self._loaded_docs[key]

    def get_existing_doc_by_type(self, doc_type):
        if not self._loaded_docs:
            self.load_data()

        filtered_keys = set([k for k, v in self._loaded_docs.items() if v['type'] in doc_type])
        available_keys = filtered_keys.difference(self._used_docs)
        key = random.choice(list(available_keys))            
        self._used_docs.add(key)
        return key, self._loaded_docs[key]

    def get_json_data_by_type(self, json_type):
        _, sample_json = self.load_data_from_file()
        data = sample_json.get(json_type, None)
        if data and 'results' in data:
            return data['results']

        return None

    def load_data(self):
        data_types, sample_json = self.get_data_from_file()
        for dt in data_types:
            data = sample_json.get(dt, None)

            if data and "results" in data:
                stable = False
                for _ in range(3):
                    try:
                        for _, content in enumerate(data["results"]):
                            key = f"{content['type']}_{content['id']}"
                            _ = self.collection.upsert(key, content)
                            self._loaded_docs[key] = content

                        stable = True
                        break
                    except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                        time.sleep(3)
                        continue
                    except Exception as ex:
                        print(ex)
                        raise

                self.skip_if_mock_unstable(stable)

    def load_extra_docs_from_file(self):
        # **NOTE** these data types all have a 'geo' object in the document, that is important for some tests
        data_types = ['airports', 'hotels', 'landmarks']
        sample_json = []
        with open(os.path.join(pathlib.Path(__file__).parent, 'travel_sample_extra_data.json')) as data_file:
            sample_data = data_file.read()
            sample_json = json.loads(sample_data)

        for dt in data_types:
            data = sample_json.get(dt, None)

            if data and 'results' in data:
                for doc in data['results']:
                    self._extra_docs[f"{doc['type']}_{doc['id']}"] = doc

    def purge_data(self):
        for k in self._loaded_docs.keys():
            try:
                self.collection.remove(k)
            except CouchbaseException:
                pass
            except Exception:
                raise

        for k in self._used_extras:
            try:
                self.collection.remove(k)
            except CouchbaseException:
                pass
            except Exception:
                raise

        self._used_extras.clear()
        self._used_docs.clear()

    def skip_if_mock_unstable(self,
                              stable # type: bool
                            ) -> None:
        if not stable and self.is_mock_server:
            pytest.skip(('CAVES does not seem to be happy. Skipping tests as failure is not'
                        ' an accurate representation of the state of the test, but rather'
                         ' there is an environment issue.'))

    @classmethod  # noqa: C901
    def get_environment(cls, **kwargs # type: Dict[str, Any]  # noqa: C901
                        ) -> TestEnvironment:  # noqa: C901

        test_suite = kwargs.pop('test_suite', None)
        if test_suite is None:
            raise CouchbaseTestEnvironmentException('No test suite provided.')
        config = kwargs.get('couchbase_config', None)
        if config is None:
            raise CouchbaseTestEnvironmentException('No test config provided.')

        coll_type = kwargs.pop('coll_type', CollectionType.DEFAULT)

        # this will only return False _if_ using the mock server
        mock_supports = EnvironmentFeatures.mock_supports_feature(test_suite.split('.')[-1],
                                                                       config.is_mock_server,
                                                                       config.is_protostellar)
        if not mock_supports:
            pytest.skip(f'Mock server does not support feature(s) required for test suite: {test_suite}')

        conn_string = config.get_connection_string()
        username, pw = config.get_username_and_pw()
        opts = ClusterOptions(PasswordAuthenticator(username, pw))
        
        tracer = kwargs.pop('tracer', None)            
        if tracer:
            opts['tracer'] = tracer
        
        meter = kwargs.pop('meter', None)
        if meter:
            opts['meter'] = meter
        
        transcoder = kwargs.pop('transcoder', None)
        if transcoder:
            opts['transcoder'] = transcoder

        okay = False
        env_args = {}
        for _ in range(3):
            try:
                cluster = Cluster.connect(conn_string, opts)
                env_args['cluster'] = cluster
                bucket = cluster.bucket(f'{config.bucket_name}')
                env_args['bucket'] = bucket
                if cluster.api_implementation == ApiImplementation.CLASSIC:
                    cluster.cluster_info()
                okay = True
                break
            except (UnAmbiguousTimeoutException, AmbiguousTimeoutException):
                continue
            except FeatureUnavailableException:
                # protostellar does not support cluster_info ATM
                okay = True
                break

        if not okay and config.is_mock_server:
            pytest.skip(('CAVES does not seem to be happy. Skipping tests as failure is not'
                        ' an accurate representation of the state of the test, but rather'
                         ' there is an environment issue.'))

        env_args['collection'] = bucket.default_collection()
        env_args.update(**kwargs)
        if coll_type == CollectionType.NAMED:
            if 'manage_collections' not in kwargs:
                kwargs['manage_collections'] = True

        cb_env = cls(**env_args)
        return cb_env

    @staticmethod
    def try_n_times(num_times,  # type: int
                     seconds_between,  # type: Union[int, float]
                     func,  # type: Callable
                     *args,  # type: Any
                     **kwargs  # type: Dict[str, Any]
                     ) -> Any:
        for _ in range(num_times):
            try:
                return func(*args, **kwargs)
            except Exception:
                print(f'trying {func} failed, sleeping for {seconds_between} seconds...')
                time.sleep(seconds_between)

    @staticmethod
    def try_n_times_till_exception(num_times,  # type: int
                                     seconds_between,  # type: Union[int, float]
                                     func,  # type: Callable
                                     *args,  # type: Any
                                     expected_exceptions=(Exception,),  # type: Tuple[Type[Exception],...]
                                     raise_exception=False,  # type: Optional[bool]
                                     **kwargs  # type: Dict[str, Any]
                                     ) -> None:
        for _ in range(num_times):
            try:
                func(*args, **kwargs)
                time.sleep(seconds_between)
            except expected_exceptions:
                if raise_exception:
                    raise
                # helpful to have this print statement when tests fail
                return
            except Exception:
                raise
