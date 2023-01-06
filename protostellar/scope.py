from typing import Optional

from protostellar.collection import Collection
from protostellar.transcoder import Transcoder


class Scope:
    def __init__(self, bucket, name):
        self._bucket = bucket
        self._scope_name = name

    @property
    def channel(self):
        """
        **INTERNAL**
        """
        return self._bucket.channel

    @property
    def name(self) -> str:
        return self._scope_name

    @property
    def bucket_name(self) -> str:
        return self._bucket.name

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._bucket.default_transcoder

    def collection(self, name  # type: str
                   ) -> Collection:
        return Collection(self, name)

    @staticmethod
    def default_name():
        return "_default"
