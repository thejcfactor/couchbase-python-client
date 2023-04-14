#  Copyright 2016-2023. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the 'License')
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an 'AS IS' BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations
from typing import Dict, Optional, Union

from new_couchbase.durability import ClientDurability, DurabilityLevel, DurabilityType, ServerDurability
from new_couchbase.exceptions import InvalidArgumentException
from new_couchbase.protostellar.proto.couchbase.kv.v1 import kv_pb2

class DurabilityParser:
    @staticmethod
    def parse_durability(durability  # type: DurabilityType
                         ) -> Optional[Union[int, Dict[str, int]]]:
        if isinstance(durability, ClientDurability):
            return {
                'num_replicated': durability.replicate_to.value,
                'num_persisted': durability.persist_to.value
            }

        if isinstance(durability, ServerDurability):
            return to_protostellar_durability_level(durability.level)

        return None

def to_protostellar_durability_level(level # type: DurabilityLevel
                                     ) -> kv_pb2.DurabilityLevel:
    if level == DurabilityLevel.MAJORITY:
        return kv_pb2.DurabilityLevel.DURABILITY_LEVEL_MAJORITY
    elif level == DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
        return kv_pb2.DurabilityLevel.DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE
    elif level == DurabilityLevel.NONE:
        raise InvalidArgumentException('Protostellar does not support DurabilityLevel.NONE')
    elif level == DurabilityLevel.PERSIST_TO_MAJORITY:
        return kv_pb2.DurabilityLevel.DURABILITY_LEVEL_PERSIST_TO_MAJORITY
    else:
        raise InvalidArgumentException(f"Expected value of type DurabilityLevel but got {type(level)}.")
    
def durability_level_from_protostellar(level # type: kv_pb2.DurabilityLevel
                                       ) -> DurabilityLevel:
    if level == kv_pb2.DurabilityLevel.DURABILITY_LEVEL_MAJORITY:
        return DurabilityLevel.MAJORITY
    elif level == kv_pb2.DurabilityLevel.DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE:
        return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
    elif level == kv_pb2.DurabilityLevel.DURABILITY_LEVEL_PERSIST_TO_MAJORITY:
        return DurabilityLevel.PERSIST_TO_MAJORITY
    else:
        raise InvalidArgumentException(f"Expected value of type DurabilityLevel but got {type(level)}.")