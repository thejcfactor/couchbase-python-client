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

import time
from typing import (Any,
                    Callable,
                    Dict,
                    Optional,
                    Tuple,
                    Type,
                    Union)
from urllib.parse import urlparse

import pytest

from couchbase.exceptions import (AmbiguousTimeoutException,
                                  BucketAlreadyExistsException,
                                  BucketDoesNotExistException,
                                  CollectionAlreadyExistsException,
                                  CouchbaseException,
                                  ScopeAlreadyExistsException,
                                  ScopeNotFoundException,
                                  UnAmbiguousTimeoutException)
from protostellar.cluster import Cluster
from protostellar.options import ConnectOptions
from protostellar.scope import Scope
from protostellar.management.buckets import CreateBucketSettings, BucketType, BucketManager
from tests.helpers import CollectionType  # noqa: F401
from tests.helpers import KVPair  # noqa: F401
from tests.helpers import (CouchbaseTestEnvironment,
                           CouchbaseTestEnvironmentException,
                           RateLimitData)


class TestEnvironment(CouchbaseTestEnvironment):

    TEST_SCOPE = "test-scope"
    TEST_COLLECTION = "test-collection"

    def __init__(self, cluster, bucket, collection, cluster_config, **kwargs):
        super().__init__(cluster, bucket, collection, cluster_config)

        if kwargs.get("manage_buckets", False) is True:
            self.check_if_feature_supported('basic_bucket_mgmt')
            self._bm = self.cluster.buckets()

        if kwargs.get("manage_collections", False) is True:
            self.check_if_feature_supported('collections')
            # self._cm = self.bucket.collections()

        self._test_bucket = None
        self._test_bucket_cm = None
        self._collection_spec = None
        self._scope = None
        self._named_collection = None

    @property
    def collection(self):
        if self._named_collection is None:
            return super().collection
        return self._named_collection

    @property
    def scope(self) -> Optional[Scope]:
        return self._scope

    @property
    def fqdn(self) -> Optional[str]:
        return f'`{self.bucket.name}`.`{self.scope.name}`.`{self.collection.name}`'

    @property
    def bm(self) -> Optional[BucketManager]:
        """Returns the default bucket's BucketManager"""
        return self._bm if hasattr(self, '_bm') else None

    # @property
    # def cm(self) -> Optional[CollectionManager]:
    #     """Returns the default CollectionManager"""
    #     return self._cm if hasattr(self, '_cm') else None

    @classmethod  # noqa: C901
    def get_environment(cls, test_suite, couchbase_config, coll_type=CollectionType.DEFAULT, **kwargs):  # noqa: C901

        # this will only return False _if_ using the mock server
        is_protostellar = test_suite.split('.')[0] == 'protostellar'
        mock_supports = CouchbaseTestEnvironment.mock_supports_feature(test_suite.split('.')[-1],
                                                                       couchbase_config.is_mock_server,
                                                                       is_protostellar=is_protostellar)
        if not mock_supports:
            if is_protostellar:
                pytest.skip(f'Mock server does not support Protostellar')
            else:
                pytest.skip(f'Mock server does not support feature(s) required for test suite: {test_suite}')

        conn_string = couchbase_config.get_connection_string(is_protostellar=True)
        username, pw = couchbase_config.get_username_and_pw()

        okay = False
        for _ in range(3):
            try:
                cluster = Cluster.connect(conn_string, ConnectOptions(username=username, password=pw))
                bucket = cluster.bucket(f'{couchbase_config.bucket_name}')
                # cluster.cluster_info()
                okay = True
                break
            except (UnAmbiguousTimeoutException, AmbiguousTimeoutException):
                continue

        if not okay:
            pytest.skip('Unable to connect to cluster.')

        coll = bucket.default_collection()
        if coll_type == CollectionType.DEFAULT:
            cb_env = cls(cluster, bucket, coll, couchbase_config, **kwargs)
        elif coll_type == CollectionType.NAMED:
            if 'manage_collections' not in kwargs:
                kwargs['manage_collections'] = True
            cb_env = cls(cluster,
                         bucket,
                         coll,
                         couchbase_config,
                         **kwargs)

        return cb_env

    def get_new_key_value(self, reset=True, debug_log=False):
        if reset is True:
            try:
                if debug_log is True:
                    self.collection.remove(self.NEW_KEY, print_kwargs=True)
                else:
                    self.collection.remove(self.NEW_KEY)
            except BaseException:
                pass
        return self.NEW_KEY, self.NEW_CONTENT

    # standard data load/purge

    # TODO: *_multi() methods??
    def load_data(self):
        data_types, sample_json = self.load_data_from_file()
        for dt in data_types:
            data = sample_json.get(dt, None)

            if data and "results" in data:
                stable = False
                for _ in range(3):
                    try:
                        for idx, r in enumerate(data["results"]):
                            key = f"{r['type']}_{r['id']}"
                            _ = self.collection.upsert(key, r)
                            self._loaded_keys.append(key)

                        stable = True
                        break
                    except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                        time.sleep(3)
                        continue
                    except Exception as ex:
                        print(ex)
                        raise

                self.skip_if_mock_unstable(stable)

    def get_json_data_by_type(self, json_type):
        _, sample_json = self.load_data_from_file()
        data = sample_json.get(json_type, None)
        if data and 'results' in data:
            return data['results']

        return None

    def purge_data(self):
        for k in self._loaded_keys:
            try:
                self.collection.remove(k)
            except CouchbaseException:
                pass


    # Bucket MGMT

    def create_bucket(self, bucket_name):
        try:
            self.bm.create_bucket(
                CreateBucketSettings(
                    name=bucket_name,
                    bucket_type=BucketType.COUCHBASE,
                    ram_quota_mb=100))
        except BucketAlreadyExistsException:
            pass
        self.try_n_times(10, 1, self.bm.get_bucket, bucket_name)

    def purge_buckets(self, buckets):
        for bucket in buckets:
            try:
                self.bm.drop_bucket(bucket)
            except BucketDoesNotExistException:
                pass
            except Exception:
                raise

            # now be sure it is really gone
            self.try_n_times_till_exception(10,
                                            3,
                                            self.bm.get_bucket,
                                            bucket,
                                            expected_exceptions=(BucketDoesNotExistException))

    # helper methods

    def sleep(self, sleep_seconds  # type: float
              ) -> None:
        time.sleep(sleep_seconds)

    def _try_n_times(self,
                     num_times,  # type: int
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
                self.sleep(seconds_between)

    def try_n_times(self,
                    num_times,  # type: int
                    seconds_between,  # type: Union[int, float]
                    func,  # type: Callable
                    *args,  # type: Any
                    reset_on_timeout=False,  # type: Optional[bool]
                    reset_num_times=None,  # type: Optional[int]
                    **kwargs  # type: Dict[str, Any]
                    ) -> Any:
        if reset_on_timeout:
            reset_times = reset_num_times or num_times
            for _ in range(reset_times):
                try:
                    return self._try_n_times(num_times,
                                             seconds_between,
                                             func,
                                             *args,
                                             **kwargs)
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    continue
                except Exception:
                    raise
        else:
            return self._try_n_times(num_times,
                                     seconds_between,
                                     func,
                                     *args,
                                     **kwargs)

        raise CouchbaseTestEnvironmentException(
            f"Unsuccessful execution of {func.__name__} after {num_times} times, "
            f"waiting {seconds_between} seconds between calls.")

    def _try_n_times_until_exception(self,
                                     num_times,  # type: int
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

    def try_n_times_till_exception(self,
                                   num_times,  # type: int
                                   seconds_between,  # type: Union[int, float]
                                   func,  # type: Callable
                                   *args,  # type: Any
                                   expected_exceptions=(Exception,),  # type: Tuple[Type[Exception],...]
                                   raise_exception=False,  # type: Optional[bool]
                                   raise_if_no_exception=True,  # type: Optional[bool]
                                   reset_on_timeout=False,  # type: Optional[bool]
                                   reset_num_times=None,  # type: Optional[int]
                                   **kwargs  # type: Dict[str, Any]
                                   ) -> None:
        if reset_on_timeout:
            reset_times = reset_num_times or num_times
            for _ in range(reset_times):
                try:
                    self._try_n_times_until_exception(num_times,
                                                      seconds_between,
                                                      func,
                                                      *args,
                                                      expected_exceptions=expected_exceptions,
                                                      raise_exception=raise_exception,
                                                      **kwargs)
                    return
                except (AmbiguousTimeoutException, UnAmbiguousTimeoutException):
                    continue
                except Exception:
                    raise
        else:
            self._try_n_times_until_exception(num_times,
                                              seconds_between,
                                              func,
                                              *args,
                                              expected_exceptions=expected_exceptions,
                                              raise_exception=raise_exception,
                                              **kwargs)
            return  # success -- return now

        # TODO: option to restart mock server?
        if raise_if_no_exception is False:
            return

        raise CouchbaseTestEnvironmentException((f"Exception not raised calling {func.__name__} {num_times} times "
                                                 f"waiting {seconds_between} seconds between calls."))
