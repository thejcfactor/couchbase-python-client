from typing import Any, Dict, Optional, List, Tuple

from protostellar.collection import Collection
from protostellar.transcoder import Transcoder
from protostellar.options import QueryOptions
from protostellar.n1ql import N1QLRequest, N1QLQuery
from protostellar.result import QueryResult

from protostellar import query_grpc_module as query



class Scope:
    def __init__(self, bucket, name, metadata):
        self._bucket = bucket
        self._scope_name = name
        self._metadata = metadata
        self._query_service = query.QueryStub(self.channel)

    @property
    def channel(self):
        """
        **INTERNAL**
        """
        return self._bucket.channel

    @property
    def query_service(self):
        """
        **INTERNAL**
        """
        return self._query_service

    @property
    def name(self) -> str:
        return self._scope_name

    @property
    def bucket_name(self) -> str:
        return self._bucket.name

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._bucket.default_transcoder

    def collection(self, name,  # type: str
                   ) -> Collection:
        return Collection(self, name, self._metadata)

    def query(
        self,
        statement,  # type: str
        *options,  # type: Optional[QueryOptions]
        **kwargs  # type: Dict[str, Any]
    ) -> QueryResult:

        # opt = QueryOptions()
        # opts = list(options)
        # for o in opts:
        #     if isinstance(o, QueryOptions):
        #         opt = o
        #         opts.remove(o)

        # set the query context as this bucket and scope if not provided
        # if not ('query_context' in opt or 'query_context' in kwargs):
        #     kwargs['query_context'] = '`{}`.`{}`'.format(self.bucket_name, self.name)

        query = N1QLQuery.create_query_object(
            self._bucket.name, self.name, statement, *options, **kwargs)
        return QueryResult(N1QLRequest.generate_n1ql_request(self.query_service,
                                                             query.params))

    @staticmethod
    def default_name():
        return "_default"
