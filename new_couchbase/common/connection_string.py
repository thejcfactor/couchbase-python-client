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

import logging
import warnings
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional,
                    Tuple,
                    Union)
from urllib.parse import parse_qs, urlparse

from new_couchbase.common.options import TLSVerifyMode

LEGACY_CONNSTR_QUERY_ARGS = {
    'ssl': {'tls_verify': TLSVerifyMode.to_str},
    'certpath': {'cert_path': lambda x: x},
    'cert_path': {'cert_path': lambda x: x},
    'truststorepath': {'trust_store_path': lambda x: x},
    'trust_store_path': {'trust_store_path': lambda x: x},
    'sasl_mech_force': {'allowed_sasl_mechanisms': lambda x: x.split(',')}
}

DEFAULT_PROTOSTELLAR_TLS_PORT = 18098

def parse_connection_string(connection_str  # type: str
                                ) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """ **INTERNAL**
    
    Parse the provided connection string

    The provided connection string will be parsed to split the connection string
    and the the query options.  Query options will be split into legacy options
    and 'current' options.

    Args:
        connection_str (str): The connection string for the cluster.

    Returns:
        Tuple[str, Dict[str, Any], Dict[str, Any]]: The parsed connection string,
            current options and legacy options.
    """
    # handle possible lack of URL scheme
    if '//' not in connection_str:
        warning_msg = 'Connection string has deprecated format. Start connection string with: couchbase://'
        warnings.warn(warning_msg, DeprecationWarning, stacklevel=2)
        connection_str = f'//{connection_str}'

    parsed_conn = urlparse(connection_str)
    conn_str = ''
    if parsed_conn.scheme and parsed_conn.scheme == 'protostellar':
        port = parsed_conn.port or DEFAULT_PROTOSTELLAR_TLS_PORT
        conn_str = f'{parsed_conn.scheme}://{parsed_conn.hostname}:{port}{parsed_conn.path}'
    elif parsed_conn.scheme:
        conn_str = f'{parsed_conn.scheme}://{parsed_conn.netloc}{parsed_conn.path}'
    else:
        conn_str = f'{parsed_conn.netloc}{parsed_conn.path}'
    query_str = parsed_conn.query
    options = parse_qs(query_str)
    # @TODO:  issue warning if it is overriding cluster options?
    legacy_query_str_opts = {k: v[0] for k, v in options.items() if k in LEGACY_CONNSTR_QUERY_ARGS.keys()}
    query_str_opts = {k: v[0] for k, v in options.items() if k not in LEGACY_CONNSTR_QUERY_ARGS.keys()}
    return conn_str, query_str_opts, legacy_query_str_opts

def parse_legacy_query_options(**query_opts  # type: Dict[str, Any]
                                ) -> Dict[str, Any]:
    """ **INTERNAL**
    Parse legacy query string options

    See :attr:`~.LEGACY_CONNSTR_QUERY_ARGS`

    Returns:
        Dict[str, Any]: Representation of parsed query string parameters.
    """
    final_options = {}
    for opt_key, opt_value in query_opts.items():
        if opt_key not in LEGACY_CONNSTR_QUERY_ARGS:
            continue
        for final_key, transform in LEGACY_CONNSTR_QUERY_ARGS[opt_key].items():
            converted = transform(opt_value)
            if converted is not None:
                final_options[final_key] = converted
    return final_options