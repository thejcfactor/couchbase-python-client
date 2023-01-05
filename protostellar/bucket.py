from typing import Optional

from couchbase.serializer import Serializer

from protostellar.scope import Scope
from protostellar.collection import Collection
from protostellar.transcoder import Transcoder


class Bucket:
    def __init__(self, cluster, name):
        self._cluster = cluster
        self._channel = cluster.channel
        self._bucket_name = name


    @property
    def channel(self):
        """
        **INTERNAL**
        """
        return self._channel

    @property
    def name(self):
        return self._bucket_name

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._cluster.default_transcoder

    @property
    def default_serializer(self) -> Optional[Serializer]:
        return self._cluster.default_serializer

    def default_scope(self) -> Scope:
        return self.scope(Scope.default_name())

    def scope(self, name  # type: str
              ) -> Scope:
        return Scope(self, name)

    def collection(self, collection_name  # type: str
                   ) -> Collection:
        scope = self.default_scope()
        return scope.collection(collection_name)

    def default_collection(self) -> Collection:
        scope = self.default_scope()
        return scope.collection(Collection.default_name())        