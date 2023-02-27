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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional,
                    Union)

from new_couchbase.api import ApiImplementation

from new_couchbase.result import (ExistsResult,
                                  GetReplicaResult,
                                  GetResult,
                                  LookupInResult,
                                  MutateInResult,
                                  MutationResult)


if TYPE_CHECKING:
    from datetime import timedelta

    from new_couchbase.common._utils import JSONType
    from new_couchbase.classic.scope import Scope as ClassicScope
    from new_couchbase.protostellar.scope import Scope as ProtostellarScope
    from new_couchbase.options import (ExistsOptions,
                                       GetOptions,
                                       GetAllReplicasOptions,
                                       GetAndLockOptions,
                                       GetAndTouchOptions,
                                       GetAnyReplicaOptions,
                                       InsertOptions,
                                       LookupInOptions,
                                       MutateInOptions,
                                       RemoveOptions,
                                       ReplaceOptions,
                                       TouchOptions,
                                       UnlockOptions,
                                       UpsertOptions)
    from new_couchbase.subdocument import Spec

class Collection:
    def __init__(self, 
                 scope, # type: Union[ClassicScope, ProtostellarScope]
                 collection_name # type: str
            ):
        self._scope = scope
        if scope.api_implementation == ApiImplementation.PROTOSTELLAR:
            from new_couchbase.protostellar.collection import Collection
            self._impl = Collection(scope, collection_name)
        else:
            from new_couchbase.classic.collection import Collection
            self._impl = Collection(scope, collection_name)

    @property
    def api_implementation(self) -> ApiImplementation:
        return self._impl.api_implementation

    @property
    def name(self) -> str:
        return self._impl.name

    def exists(
        self,
        key,  # type: str
        *opts,  # type: ExistsOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> ExistsResult:
        """Checks whether a specific document exists or not.

        Args:
            key (str): The key for the document to check existence.
            opts (:class:`~couchbase.options.ExistsOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ExistsOptions`

        Returns:
            :class:`~couchbase.result.ExistsResult`: An instance of :class:`~couchbase.result.ExistsResult`.

        Examples:

            Simple exists operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = collection.exists(key)
                print(f'Document w/ key - {key} {"exists" if res.exists else "does not exist"}')


            Simple exists operation with options::

                from datetime import timedelta
                from couchbase.options import ExistsOptions

                # ... other code ...

                key = 'airline_10'
                res = collection.exists(key, ExistsOptions(timeout=timedelta(seconds=2)))
                print(f'Document w/ key - {key} {"exists" if res.exists else "does not exist"}')

        """
        return ExistsResult(self._impl.exists(key, *opts, **kwargs))

    def get(self,
            key,  # type: str
            *opts,  # type: GetOptions
            **kwargs,  # type: Dict[str, Any]
            ) -> GetResult:
        """Retrieves the value of a document from the collection.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetOptions`

        Returns:
            :class:`~couchbase.result.GetResult`: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple get operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = collection.get('airline_10')
                print(f'Document value: {res.content_as[dict]}')


            Simple get operation with options::

                from datetime import timedelta
                from couchbase.options import GetOptions

                # ... other code ...

                res = collection.get('airline_10', GetOptions(timeout=timedelta(seconds=2)))
                print(f'Document value: {res.content_as[dict]}')

        """
        return GetResult(self._impl.get(key, *opts, **kwargs))

    def get_all_replicas(self,
                         key,  # type: str
                         *opts,  # type: GetAllReplicasOptions
                         **kwargs,  # type: Dict[str, Any]
                         ) -> Iterable[GetReplicaResult]:
        """Retrieves the value of a document from the collection returning both active and all available replicas.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetAllReplicasOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAllReplicasOptions`

        Returns:
            Iterable[:class:`~couchbase.result.GetReplicaResult`]: A stream of
            :class:`~couchbase.result.GetReplicaResult` representing both active and replicas of the document retrieved.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
            on the server.

        Examples:

            Simple get_all_replicas operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                result = collection.get_all_replicas('airline_10')
                for res in results:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Document value: {res.content_as[dict]}')


            Simple get_all_replicas operation with options::

                from datetime import timedelta
                from couchbase.options import GetAllReplicasOptions

                # ... other code ...

                result = collection.get_all_replicas('airline_10', GetAllReplicasOptions(timeout=timedelta(seconds=10)))
                for res in result:
                    print(f'Document is replica: {res.is_replica}')
                    print(f'Document value: {res.content_as[dict]}')

            Stream get_all_replicas results::

                from datetime import timedelta
                from couchbase.options import GetAllReplicasOptions

                # ... other code ...

                result = collection.get_all_replicas('airline_10', GetAllReplicasOptions(timeout=timedelta(seconds=10)))
                while True:
                    try:
                        res = next(result)
                        print(f'Document is replica: {res.is_replica}')
                        print(f'Document value: {res.content_as[dict]}')
                    except StopIteration:
                        print('Done streaming replicas.')
                        break

        """
        return [GetReplicaResult(r) for r in self._impl.get_all_replicas(key, *opts, **kwargs)]

    def get_and_lock(self,
                     key,  # type: str
                     lock_time,  # type: timedelta
                     *opts,  # type: GetAndLockOptions
                     **kwargs,  # type: Dict[str, Any]
                     ) -> GetResult:
        """Locks a document and retrieves the value of that document at the time it is locked.

        Args:
            key (str): The key for the document to lock and retrieve.
            lock_time (timedelta):  The amount of time to lock the document.
            opts (:class:`~couchbase.options.GetAndLockOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndLockOptions`

        Returns:
            :class:`~couchbase.result.GetResult`: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple get and lock operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = collection.get_and_lock(key, timedelta(seconds=20))
                print(f'Locked document: {res.content_as[dict]}')


            Simple get and lock operation with options::

                from datetime import timedelta
                from couchbase.options import GetAndLockOptions

                # ... other code ...

                key = 'airline_10'
                res = collection.get_and_lock(key,
                                            timedelta(seconds=20),
                                            GetAndLockOptions(timeout=timedelta(seconds=2)))
                print(f'Locked document: {res.content_as[dict]}')

        """
        return GetResult(self._impl.get_and_lock(key, lock_time, *opts, **kwargs))

    def get_and_touch(self,
                      key,  # type: str
                      expiry,  # type: timedelta
                      *opts,  # type: GetAndTouchOptions
                      **kwargs,  # type: Dict[str, Any]
                      ) -> GetResult:
        """Retrieves the value of the document and simultanously updates the expiry time for the same document.

        Args:
            key (str): The key for the document retrieve and set expiry time.
            expiry (timedelta):  The new expiry to apply to the document.
            opts (:class:`~couchbase.options.GetAndTouchOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAndTouchOptions`

        Returns:
            :class:`~couchbase.result.GetResult`: An instance of :class:`~couchbase.result.GetResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple get and touch operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = collection.get_and_touch(key, timedelta(seconds=20))
                print(f'Document w/ updated expiry: {res.content_as[dict]}')


            Simple get and touch operation with options::

                from datetime import timedelta
                from couchbase.options import GetAndTouchOptions

                # ... other code ...

                key = 'airline_10'
                res = collection.get_and_touch(key,
                                            timedelta(seconds=20),
                                            GetAndTouchOptions(timeout=timedelta(seconds=2)))
                print(f'Document w/ updated expiry: {res.content_as[dict]}')

        """
        return GetResult(self._impl.get_and_touch(key, expiry, *opts, **kwargs))

    def get_any_replica(self,
                        key,  # type: str
                        *opts,  # type: GetAnyReplicaOptions
                        **kwargs,  # type: Dict[str, Any]
                        ) -> GetReplicaResult:
        """Retrieves the value of a document from the collection leveraging both active and all available replicas returning
        the first available.

        Args:
            key (str): The key for the document to retrieve.
            opts (:class:`~couchbase.options.GetAnyReplicaOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.GetAnyReplicaOptions`

        Returns:
            :class:`~couchbase.result.GetReplicaResult`: An instance of :class:`~couchbase.result.GetReplicaResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentUnretrievableException`: If the key provided does not exist
                on the server.

        Examples:

            Simple get_any_replica operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = collection.get_any_replica('airline_10')
                print(f'Document is replica: {res.is_replica}')
                print(f'Document value: {res.content_as[dict]}')


            Simple get_any_replica operation with options::

                from datetime import timedelta
                from couchbase.options import GetAnyReplicaOptions

                # ... other code ...

                res = collection.get_any_replica('airline_10', GetAnyReplicaOptions(timeout=timedelta(seconds=5)))
                print(f'Document is replica: {res.is_replica}')
                print(f'Document value: {res.content_as[dict]}')

        """
        return GetReplicaResult(self._impl.get_any_replica(key, *opts, **kwargs))

    def insert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: InsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        """Inserts a new document to the collection, failing if the document already exists.

        Args:
            key (str): Document key to insert.
            value (JSONType): The value of the document to insert.
            opts (:class:`~couchbase.options.InsertOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.InsertOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentExistsException`: If the document already exists on the
                server.

        Examples:

            Simple insert operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_8091'
                airline = {
                    "type": "airline",
                    "id": 8091,
                    "callsign": "CBS",
                    "iata": None,
                    "icao": None,
                    "name": "Couchbase Airways",
                }
                res = collection.insert(key, doc)


            Simple insert operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import InsertOptions

                # ... other code ...

                key = 'airline_8091'
                airline = {
                    "type": "airline",
                    "id": 8091,
                    "callsign": "CBS",
                    "iata": None,
                    "icao": None,
                    "name": "Couchbase Airways",
                }
                durability = ServerDurability(level=DurabilityLevel.PERSIST_TO_MAJORITY)
                res = collection.insert(key, doc, InsertOptions(durability=durability))

        """
        return MutationResult(self._impl.insert(key, value, *opts, **kwargs))

    def lookup_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: LookupInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> LookupInResult:
        """Performs a lookup-in operation against a document, fetching individual fields or information
        about specific fields inside the document value.

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the data to fetch
                from the document.
            opts (:class:`~couchbase.options.LookupInOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.LookupInOptions`

        Returns:
            :class:`~couchbase.result.LookupInResult`: An instance of :class:`~couchbase.result.LookupInResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple look-up in operation::

                import couchbase.subdocument as SD

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('hotel')

                key = 'hotel_10025'
                res = collection.lookup_in(key, (SD.get("geo"),))
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')


            Simple look-up in operation with options::

                from datetime import timedelta

                import couchbase.subdocument as SD
                from couchbase.options import LookupInOptions

                # ... other code ...

                key = 'hotel_10025'
                res = collection.lookup_in(key,
                                            (SD.get("geo"),),
                                            LookupInOptions(timeout=timedelta(seconds=2)))
                print(f'Hotel {key} coordinates: {res.content_as[dict](0)}')

        """
        return LookupInResult(self._impl.lookup_in(key, spec, *opts, **kwargs))

    def mutate_in(
        self,
        key,  # type: str
        spec,  # type: Iterable[Spec]
        *opts,  # type: MutateInOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutateInResult:
        """Performs a mutate-in operation against a document. Allowing atomic modification of specific fields
        within a document. Also enables access to document extended-attributes (i.e. xattrs).

        Args:
            key (str): The key for the document look in.
            spec (Iterable[:class:`~couchbase.subdocument.Spec`]):  A list of specs describing the operations to
                perform on the document.
            opts (:class:`~couchbase.options.MutateInOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.MutateInOptions`

        Returns:
            :class:`~couchbase.result.MutateInResult`: An instance of :class:`~couchbase.result.MutateInResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple mutate-in operation::

                import couchbase.subdocument as SD

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('hotel')

                key = 'hotel_10025'
                res = collection.mutate_in(key, (SD.replace("city", "New City"),))


            Simple mutate-in operation with options::

                from datetime import timedelta

                import couchbase.subdocument as SD
                from couchbase.options import MutateInOptions

                # ... other code ...

                key = 'hotel_10025'
                res = collection.mutate_in(key,
                                            (SD.replace("city", "New City"),),
                                            MutateInOptions(timeout=timedelta(seconds=2)))

        """
        return MutateInResult(self._impl.mutate_in(key, spec, *opts, **kwargs))  

    def remove(self,
               key,  # type: str
               *opts,  # type: RemoveOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> MutationResult:
        """Removes an existing document. Failing if the document does not exist.

        Args:
            key (str): Key for the document to remove.
            opts (:class:`~couchbase.options.RemoveOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.RemoveOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.

        Examples:

            Simple remove operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = collection.remove('airline_10')


            Simple remove operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import RemoveOptions

                # ... other code ...

                durability = ServerDurability(level=DurabilityLevel.MAJORITY)
                res = collection.remove('airline_10', RemoveOptions(durability=durability))

        """
        return MutationResult(self._impl.remove(key, *opts, **kwargs))

    def replace(self,
                key,  # type: str
                value,  # type: JSONType
                *opts,  # type: ReplaceOptions
                **kwargs,  # type: Dict[str, Any]
                ) -> MutationResult:
        """Replaces the value of an existing document. Failing if the document does not exist.

        Args:
            key (str): Document key to replace.
            value (JSONType): The value of the document to replace.
            opts (:class:`~couchbase.options.ReplaceOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.ReplaceOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.

        Examples:

            Simple replace operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_8091'
                res = collection.get(key)
                content = res.content_as[dict]
                airline["name"] = "Couchbase Airways!!"
                res = collection.replace(key, doc)


            Simple replace operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import ReplaceOptions

                # ... other code ...

                key = 'airline_8091'
                res = collection.get(key)
                content = res.content_as[dict]
                airline["name"] = "Couchbase Airways!!"
                durability = ServerDurability(level=DurabilityLevel.MAJORITY)
                res = collection.replace(key, doc, InsertOptions(durability=durability))

        """
        return MutationResult(self._impl.replace(key, value, *opts, **kwargs))

    def touch(self,
              key,  # type: str
              expiry,  # type: timedelta
              *opts,  # type: TouchOptions
              **kwargs,  # type: Dict[str, Any]
              ) -> MutationResult:
        """Updates the expiry on an existing document.

        Args:
            key (str): Key for the document to touch.
            expiry (timedelta): The new expiry for the document.
            opts (:class:`~couchbase.options.TouchOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.TouchOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the document does not exist on the
                server.

        Examples:

            Simple touch operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                res = collection.touch('airline_10', timedelta(seconds=300))


            Simple touch operation with options::

                from datetime import timedelta

                from couchbase.options import TouchOptions

                # ... other code ...

                res = collection.touch('airline_10',
                                        timedelta(seconds=300),
                                        TouchOptions(timeout=timedelta(seconds=2)))

        """
        return MutationResult(self._impl.touch(key, expiry, *opts, **kwargs))

    def unlock(self,
               key,  # type: str
               cas,  # type: int
               *opts,  # type: UnlockOptions
               **kwargs,  # type: Dict[str, Any]
               ) -> None:
        """Unlocks a previously locked document.

        Args:
            key (str): The key for the document to unlock.
            cas (int): The CAS of the document, used to validate lock ownership.
            opts (:class:`couchbaseoptions.UnlockOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UnlockOptions`

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

            :class:`~couchbase.exceptions.DocumentLockedException`: If the provided cas is invalid.

        Examples:

            Simple unlock operation::

                from datetime import timedelta

                # ... other code ...

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_10'
                res = collection.get_and_lock(key, timedelta(seconds=5))
                collection.unlock(key, res.cas)
                # this should be okay once document is unlocked
                collection.upsert(key, res.content_as[dict])

        """
        self._impl.unlock(key, cas, *opts, **kwargs)

    def upsert(
        self,
        key,  # type: str
        value,  # type: JSONType
        *opts,  # type: UpsertOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MutationResult:
        """Upserts a document to the collection. This operation succeeds whether or not the document already exists.

        Args:
            key (str): Document key to upsert.
            value (JSONType): The value of the document to upsert.
            opts (:class:`~couchbase.options.UpsertOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.UpsertOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Examples:

            Simple upsert operation::

                bucket = cluster.bucket('travel-sample')
                collection = bucket.scope('inventory').collection('airline')

                key = 'airline_8091'
                airline = {
                    "type": "airline",
                    "id": 8091,
                    "callsign": "CBS",
                    "iata": None,
                    "icao": None,
                    "name": "Couchbase Airways",
                }
                res = collection.upsert(key, doc)


            Simple upsert operation with options::

                from couchbase.durability import DurabilityLevel, ServerDurability
                from couchbase.options import UpsertOptions

                # ... other code ...

                key = 'airline_8091'
                airline = {
                    "type": "airline",
                    "id": 8091,
                    "callsign": "CBS",
                    "iata": None,
                    "icao": None,
                    "name": "Couchbase Airways",
                }
                durability = ServerDurability(level=DurabilityLevel.MAJORITY)
                res = collection.upsert(key, doc, InsertOptions(durability=durability))

        """
        return MutationResult(self._impl.upsert(key, value, *opts, **kwargs))

    @staticmethod
    def default_name():
        return "_default"
