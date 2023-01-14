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

import copy
from typing import Any, Dict, List, Optional

from new_couchbase.common.options import OptionTypes
from new_couchbase.common._utils import (validate_serializer, validate_transcoder)
from new_couchbase.impl.protostellar._utils import timedelta_as_timestamp

from new_couchbase.exceptions import InvalidArgumentException

class ValidClusterOptions:

    _VALID_CLUSTER_OPTS = {
        'authenticator': {'authenticator': lambda x: x},
        # 'enable_tls': {'enable_tls': validate_bool},
        # 'enable_mutation_tokens': {'enable_mutation_tokens': validate_bool},
        # 'enable_tcp_keep_alive': {'enable_tcp_keep_alive': validate_bool},
        # 'ip_protocol': {'use_ip_protocol': IpProtocol.to_str},
        # 'enable_dns_srv': {'enable_dns_srv': validate_bool},
        # 'show_queries': {'show_queries': validate_bool},
        # 'enable_unordered_execution': {'enable_unordered_execution': validate_bool},
        # 'enable_clustermap_notification': {'enable_clustermap_notification': validate_bool},
        # 'enable_compression': {'enable_compression': validate_bool},
        # 'enable_tracing': {'enable_tracing': validate_bool},
        # 'enable_metrics': {'enable_metrics': validate_bool},
        # 'network': {'network': validate_str},
        # 'tls_verify': {'tls_verify': TLSVerifyMode.to_str},
        'serializer': {'serializer': validate_serializer},
        'transcoder': {'transcoder': validate_transcoder},
        # 'span': {'span': lambda x: x},
        # 'tcp_keep_alive_interval': {'tcp_keep_alive_interval': timedelta_as_microseconds},
        # 'config_poll_interval': {'config_poll_interval': timedelta_as_microseconds},
        # 'config_poll_floor': {'config_poll_floor': timedelta_as_microseconds},
        # 'max_http_connections': {'max_http_connections': validate_int},
        # 'user_agent_extra': {'user_agent_extra': validate_str},
        # 'trust_store_path': {'trust_store_path': validate_str},
        # 'cert_path': {'cert_path': validate_str},
        # 'logging_meter_emit_interval': {'emit_interval': timedelta_as_microseconds},
        # 'num_io_threads': {'num_io_threads': validate_int},
        # 'transaction_config': {'transaction_config': lambda x: x},
        # 'tracer': {'tracer': lambda x: x},
        # 'meter': {'meter': lambda x: x},
        # 'dns_nameserver': {'dns_nameserver': validate_str},
        # 'dns_port': {'dns_port': validate_int},
    }

    _VALID_TIMING_OPTS = {
        # 'bootstrap_timeout': {'bootstrap_timeout': timedelta_as_microseconds},
        # 'resolve_timeout': {'resolve_timeout': timedelta_as_microseconds},
        # 'connect_timeout': {'connect_timeout': timedelta_as_microseconds},
        # 'kv_timeout': {'key_value_timeout': timedelta_as_microseconds},
        # 'kv_durable_timeout': {'key_value_durable_timeout': timedelta_as_microseconds},
        # 'views_timeout': {'view_timeout': timedelta_as_microseconds},
        # 'query_timeout': {'query_timeout': timedelta_as_microseconds},
        # 'analytics_timeout': {'analytics_timeout': timedelta_as_microseconds},
        # 'search_timeout': {'search_timeout': timedelta_as_microseconds},
        # 'management_timeout': {'management_timeout': timedelta_as_microseconds},
        # 'dns_srv_timeout': {'dns_srv_timeout': timedelta_as_microseconds},
        # 'idle_http_connection_timeout': {'idle_http_connection_timeout': timedelta_as_microseconds},
        # 'config_idle_redial_timeout': {'config_idle_redial_timeout': timedelta_as_microseconds}
    }

    _VALID_TRACING_OPTS = {
        # 'tracing_threshold_kv': {'key_value_threshold': timedelta_as_microseconds},
        # 'tracing_threshold_view': {'view_threshold': timedelta_as_microseconds},
        # 'tracing_threshold_query': {'query_threshold': timedelta_as_microseconds},
        # 'tracing_threshold_search': {'search_threshold': timedelta_as_microseconds},
        # 'tracing_threshold_analytics': {'analytics_threshold': timedelta_as_microseconds},
        # 'tracing_threshold_eventing': {'eventing_threshold': timedelta_as_microseconds},
        # 'tracing_threshold_management': {'management_threshold': timedelta_as_microseconds},
        # 'tracing_threshold_queue_size': {'threshold_sample_size': validate_int},
        # 'tracing_threshold_queue_flush_interval': {'threshold_emit_interval': timedelta_as_microseconds},
        # 'tracing_orphaned_queue_size': {'orphaned_sample_size': validate_int},
        # 'tracing_orphaned_queue_flush_interval': {'orphaned_emit_interval': timedelta_as_microseconds}
    }


    @staticmethod
    def get_valid_options(opt_type # type: OptionTypes
        ) -> Dict[str, Any]:

        valid_opts = {}
        if opt_type == OptionTypes.Cluster:
            valid_opts = copy.copy(ValidClusterOptions._VALID_CLUSTER_OPTS)
            valid_opts.update(copy.copy(ValidClusterOptions._VALID_TIMING_OPTS))
            valid_opts.update(copy.copy(ValidClusterOptions._VALID_TRACING_OPTS))
        elif opt_type == OptionTypes.ClusterTimeout:
            valid_opts = copy.copy(ValidClusterOptions._VALID_TIMING_OPTS)
        elif opt_type == OptionTypes.ClusterTracing:
            valid_opts = copy.copy(ValidClusterOptions._VALID_TRACING_OPTS)
        else:
            raise InvalidArgumentException(f"Invalid option type: {opt_type}")

        return valid_opts

    @staticmethod
    def get_allowed_option_keys(opt_type, # type: str
                                use_transform_keys=False  # type: Optional[bool]
                                ) -> List[str]:

        if opt_type == OptionTypes.Cluster:
            if use_transform_keys is True:
                keys = []
                for val in ValidClusterOptions._VALID_CLUSTER_OPTS.values():
                    keys.append(list(val.keys())[0])
                return keys

            return list(ValidClusterOptions._VALID_CLUSTER_OPTS.keys())
        elif opt_type == OptionTypes.ClusterTimeout:
            if use_transform_keys is True:
                keys = []
                for val in ValidClusterOptions._VALID_TIMING_OPTS.values():
                    keys.append(list(val.keys())[0])
                return keys

            return list(ValidClusterOptions._VALID_TIMING_OPTS.keys())
        elif opt_type == OptionTypes.ClusterTracing:
            if use_transform_keys is True:
                keys = []
                for val in ValidClusterOptions._VALID_TRACING_OPTS.values():
                    keys.append(list(val.keys())[0])
                return keys

            return list(ValidClusterOptions._VALID_TRACING_OPTS.keys())
        else:
            raise InvalidArgumentException(f"Invalid option type: {opt_type}")

class ValidKeyValueOptions:

    _VALID_OPTS = {
        'timeout': {'timeout': timedelta_as_timestamp},
        # 'span': {'span': lambda x: x},
    }

    _VALID_DURABLE_OPTS = {
        'timeout': {'timeout': timedelta_as_timestamp},
        # 'span': {'span': lambda x: x},
        # 'durability': {'durability': DurabilityParser.parse_durability},
    }

    @staticmethod
    def get_valid_options(opt_type # type: OptionTypes
        ) -> Dict[str, Any]:

        if opt_type == OptionTypes.Exists:
            valid_opts = copy.copy(ValidKeyValueOptions._VALID_OPTS)
        elif opt_type == OptionTypes.Get:
            valid_opts = copy.copy(ValidKeyValueOptions._VALID_OPTS)
            # valid_opts.update({
            #     'project': {'with_expiry': validate_projections},
            #     'transcoder': {'transcoder': validate_transcoder},
            #     'with_expiry': {'with_expiry': validate_bool},
            # })
        elif opt_type == OptionTypes.Insert:
            valid_opts = copy.copy(ValidKeyValueOptions._VALID_DURABLE_OPTS)
            # valid_opts.update({
            #     'expiry': {'expiry': timedelta_as_timestamp},
            #     'transcoder': {'transcoder': validate_transcoder},
            # })
        elif opt_type == OptionTypes.Replace:
            valid_opts = copy.copy(ValidKeyValueOptions._VALID_DURABLE_OPTS)
            # valid_opts.update({
            #     'cas': {'cas': validate_int},
            #     'expiry': {'expiry': timedelta_as_timestamp},
            #     'preserve_expiry': {'preserve_expiry': validate_bool},
            #     'transcoder': {'transcoder': validate_transcoder},
            # })
        elif opt_type == OptionTypes.Remove:
            valid_opts = copy.copy(ValidKeyValueOptions._VALID_DURABLE_OPTS)
            # valid_opts.update({
            #     'cas': {'cas': validate_int},
            # })
        elif opt_type == OptionTypes.Touch:
            valid_opts = copy.copy(ValidKeyValueOptions._VALID_OPTS)
        elif opt_type == OptionTypes.Unlock:
            valid_opts = copy.copy(ValidKeyValueOptions._VALID_OPTS)
        elif opt_type == OptionTypes.Upsert:
            valid_opts = copy.copy(ValidKeyValueOptions._VALID_DURABLE_OPTS)
            # valid_opts.update({
            #     'expiry': {'expiry': timedelta_as_timestamp},
            #     'preserve_expiry': {'preserve_expiry': validate_bool},
            #     'transcoder': {'transcoder': validate_transcoder},
            # })
        else:
            raise InvalidArgumentException(f"Invalid option type: {opt_type}")

        return valid_opts