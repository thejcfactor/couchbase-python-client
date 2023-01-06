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
from random import choice

import pytest

from couchbase.durability import DurabilityLevel
from couchbase.exceptions import (BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  BucketNotFlushableException,
                                  FeatureUnavailableException)
from couchbase.management.buckets import (BucketSettings,
                                          BucketType,
                                          ConflictResolutionType,
                                          CreateBucketSettings,
                                          StorageBackend)

from ._test_utils import TestEnvironment

class BucketManagementTests:

    @pytest.fixture(scope="class")
    def test_buckets(self):
        return [f"test-bucket-{i}" for i in range(5)]

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config, test_buckets):
        cb_env = TestEnvironment.get_environment(__name__, couchbase_config, manage_buckets=True)
        yield cb_env
        # cb_env.purge_buckets(test_buckets)

    @pytest.fixture(scope="class")
    def check_bucket_mgmt_supported(self, cb_env):
        cb_env.check_if_feature_supported('bucket_mgmt')

    @pytest.fixture(scope="class")
    def check_bucket_min_durability_supported(self, cb_env):
        cb_env.check_if_feature_supported('bucket_min_durability')

    @pytest.fixture(scope="class")
    def check_bucket_storage_backend_supported(self, cb_env):
        cb_env.check_if_feature_supported('bucket_storage_backend')

    @pytest.fixture(scope="class")
    def check_custom_conflict_resolution_supported(self, cb_env):
        cb_env.check_if_feature_supported('custom_conflict_resolution')

    @pytest.fixture()
    def test_bucket(self, test_buckets):
        return choice(test_buckets)

    @pytest.fixture()
    def purge_buckets(self, cb_env, test_buckets):
        yield
        cb_env.purge_buckets(test_buckets)

    # @pytest.mark.usefixtures("check_bucket_mgmt_supported")
    # @pytest.mark.usefixtures("purge_buckets")
    def test_bucket_create(self, cb_env, test_bucket):
        cb_env.bm.create_bucket(
            CreateBucketSettings(
                name=test_bucket,
                bucket_type=BucketType.COUCHBASE,
                ram_quota_mb=100))
        # bucket = cb_env.try_n_times(10, 1, cb_env.bm.get_bucket, test_bucket)
        # if cb_env.server_version_short >= 6.6:
        #     assert bucket["minimum_durability_level"] == DurabilityLevel.NONE