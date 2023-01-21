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
import time
from datetime import datetime, timedelta
from typing import Union

from new_couchbase.exceptions import QueryIndexNotFoundException
from new_couchbase.result import QueryResult
from new_couchbase.protostellar.result import QueryResult as ProtostellarQueryResult
from new_couchbase.api import ApiImplementation

from .test_environment import TestEnvironment


class QueryTestEnvironment(TestEnvironment):

    def assert_rows(self,
                    result,  # type: Union[ProtostellarQueryResult, QueryResult]
                    expected_count):
        count = 0
        assert isinstance(result, (ProtostellarQueryResult, QueryResult))
        for row in result.rows():
            assert row is not None
            count += 1
        assert count >= expected_count

    def setup(self):
        # TestEnvironment.try_n_times(10, 3, self.ixm.create_primary_index,
        #                    self.bucket.name,
        #                    timeout=timedelta(seconds=60),
        #                    ignore_if_exists=True)

        for _ in range(5):
            if self.cluster.api_implementation == ApiImplementation.CLASSIC:
                row_count_good = self._check_row_count(self.cluster, self.bucket.name, 5)
            else:
                row_count_good = self._check_row_count(self.scope, self.bucket.name, 5)

            if row_count_good:
                break
            print('Waiting for index to load, sleeping a bit...')
            time.sleep(5)

    def teardown(self):
        pass
        # TestEnvironment.try_n_times_till_exception(10, 3,
        #                                   self.ixm.drop_primary_index,
        #                                   self.bucket.name,
        #                                   expected_exceptions=(QueryIndexNotFoundException))

    def _check_row_count(self,
                         cb,
                         query_namespace, # type: str
                         min_count  # type: int
                         ) -> bool:

        result = cb.query(f"SELECT * FROM `{query_namespace}` WHERE country LIKE 'United%' LIMIT 5")
        count = 0
        for _ in result.rows():
            count += 1
        return count >= min_count