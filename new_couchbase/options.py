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

from datetime import timedelta

from typing import Any, Dict, List, Optional, Union, overload, TYPE_CHECKING

from new_couchbase.common.options import AcceptableInts  # noqa: F401
from new_couchbase.common.options import Compression  # noqa: F401
from new_couchbase.common.options import IpProtocol  # noqa: F401
from new_couchbase.common.options import LockMode  # noqa: F401
from new_couchbase.common.options import TLSVerifyMode  # noqa: F401
from new_couchbase.common.options import (AppendOptionsBase,
                                          ConfigProfile, 
                                          ConstrainedIntBase,
                                          ClusterOptionsBase, 
                                          ClusterTimeoutOptionsBase, 
                                          ClusterTracingOptionsBase,
                                          DecrementOptionsBase,
                                          DeltaValueBase,
                                          ExistsOptionsBase,
                                          GetAllReplicasOptionsBase,
                                          GetAndLockOptionsBase,
                                          GetAndTouchOptionsBase,
                                          GetAnyReplicaOptionsBase,
                                          GetOptionsBase,
                                          IncrementOptionsBase,
                                          InsertOptionsBase,
                                          KnownConfigProfiles,
                                          LookupInOptionsBase,
                                          MutateInOptionsBase,
                                          OptionsBase,
                                          PrependOptionsBase, 
                                          QueryOptionsBase,
                                          RemoveOptionsBase,
                                          ReplaceOptionsBase,
                                          SignedInt64Base,
                                          TouchOptionsBase,
                                          UnlockOptionsBase,
                                          UnsignedInt32Base,
                                          UnsignedInt64Base,
                                          UpsertOptionsBase,
                                          WaitUntilReadyOptionsBase)

from new_couchbase.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from new_couchbase.api.authentication import Authenticator

"""

Python SDK Cluster Options

"""

class ClusterTracingOptions(ClusterTracingOptionsBase):
    """Available tracing options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    These will be the default timeouts for operations for the entire cluster

    Args:
        tracing_threshold_kv (timedelta, optional): KV operations threshold. Defaults to None.
        tracing_threshold_view (timedelta, optional): Views operations threshold. Defaults to None.
        tracing_threshold_query (timedelta, optional): Query operations threshold. Defaults to None.
        tracing_threshold_search (timedelta, optional): Search operations threshold.. Defaults to None.
        tracing_threshold_analytics (timedelta, optional): Analytics operations threshold. Defaults to None.
        tracing_threshold_eventing (timedelta, optional): Eventing operations threshold. Defaults to None.
        tracing_threshold_management (timedelta, optional): Management operations threshold. Defaults to None.
        tracing_threshold_queue_size (int, optional): Size of tracing operations queue. Defaults to None.
        tracing_threshold_queue_flush_interval (timedelta, optional): Interveral to flush tracing operations queue.
            Defaults to None.
        tracing_orphaned_queue_size (int, optional): Size of tracing orphaned operations queue. Defaults to None.
        tracing_orphaned_queue_flush_interval (timedelta, optional): Interveral to flush tracing orphaned operations
            queue. Defaults to None.
    """


class ClusterTimeoutOptions(ClusterTimeoutOptionsBase):
    """Available timeout options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    These will set the default timeouts for operations for the cluster.  Some operations allow the timeout to
    be overridden on a per operation basis.

    Args:
        bootstrap_timeout (timedelta, optional): bootstrap timeout. Defaults to None.
        resolve_timeout (timedelta, optional): resolve timeout. Defaults to None.
        connect_timeout (timedelta, optional): connect timeout. Defaults to None.
        kv_timeout (timedelta, optional): KV operations timeout. Defaults to None.
        kv_durable_timeout (timedelta, optional): KV durability operations timeout. Defaults to None.
        views_timeout (timedelta, optional): views operations timeout. Defaults to None.
        query_timeout (timedelta, optional): query operations timeout. Defaults to None.
        analytics_timeout (timedelta, optional): analytics operations timeout. Defaults to None.
        search_timeout (timedelta, optional): search operations timeout. Defaults to None.
        management_timeout (timedelta, optional): management operations timeout. Defaults to None.
        dns_srv_timeout (timedelta, optional): DNS SRV connection timeout. Defaults to None.
        idle_http_connection_timeout (timedelta, optional): Idle HTTP connection timeout. Defaults to None.
        config_idle_redial_timeout (timedelta, optional): Idle redial timeout. Defaults to None.
        config_total_timeout (timedelta, optional): **DEPRECATED** complete bootstrap timeout. Defaults to None.
    """

class WanDevelopmentProfile(ConfigProfile):
    """
    **VOLATILE** This API is subject to change at any time.

    The WAN Development profile sets various timeout options that are useful when develoption in a WAN environment.
    """

    def __init__(self):
        super().__init__()

    def apply(self,
              options  # type: ClusterOptions
              ) -> None:
        # Need to use keys in couchbase.logic.ClusterTimeoutOptionsBase._VALID_OPTS
        options['kv_timeout'] = timedelta(seconds=20)
        options['kv_durable_timeout'] = timedelta(seconds=20)
        options['connect_timeout'] = timedelta(seconds=20)
        options['analytics_timeout'] = timedelta(seconds=120)
        options['query_timeout'] = timedelta(seconds=120)
        options['search_timeout'] = timedelta(seconds=120)
        options['management_timeout'] = timedelta(seconds=120)
        options['views_timeout'] = timedelta(seconds=120)


class ConfigProfiles():
    """
    **VOLATILE** This API is subject to change at any time.

    The `ConfigProfiles` class is responsible for keeping track of registered/known Configuration
    Profiles.
    """

    def __init__(self):
        self._profiles = {}
        self.register_profile(KnownConfigProfiles.WanDevelopment.value, WanDevelopmentProfile())

    def apply_profile(self,
                      profile_name,  # type: str
                      options  # type: ClusterOptions
                      ) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided ConfigProfile options.

        Args:
            profile_name (str):  The name of the profile to apply.
            options (:class:`~couchbase.options.ClusterOptions`): The options to apply the ConfigProfile options
                toward. The ConfigProfile options will override any matching option(s) previously set.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.
        """
        if profile_name not in self._profiles:
            raise InvalidArgumentException(f'{profile_name} is not a registered profile.')

        self._profiles[profile_name].apply(options)

    def register_profile(self,
                         profile_name,  # type: str
                         profile,  # type: ConfigProfile
                         ) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Register a :class:`~couchbase.options.ConfigProfile`.

        Args:
            profile_name (str):  The name of the :class:`~couchbase.options.ConfigProfile` to register.
            profile (:class:`~couchbase.options.ConfigProfile`): The :class:`~couchbase.options.ConfigProfile`
                to register.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not derived
            from :class:`~couchbase.options.ConfigProfile`.

        """
        if not issubclass(profile.__class__, ConfigProfile):
            raise InvalidArgumentException('A Configuration Profile must be derived from ConfigProfile')

        self._profiles[profile_name] = profile

    def unregister_profile(self,
                           profile_name  # type: str
                           ) -> Optional[ConfigProfile]:
        """
        **VOLATILE** This API is subject to change at any time.

        Unregister a :class:`~couchbase.options.ConfigProfile`.

        Args:
            profile_name (str):  The name of the :class:`~couchbase.options.ConfigProfile` to unregister.

        Returns
            Optional(:class:`~couchbase.options.ConfigProfile`): The unregistered :class:`~couchbase.options.ConfigProfile`
        """  # noqa: E501

        return self._profiles.pop(profile_name, None)

"""
**VOLATILE** The ConfigProfiles API is subject to change at any time.
"""
CONFIG_PROFILES = ConfigProfiles()

class ClusterOptions(ClusterOptionsBase):
    """Available options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Cluster options enable the configuration of various global cluster settings.
    Some options can be set globally for the cluster, but overridden for specific
    operations (i.e. ClusterTimeoutOptions)

    .. note::

        The authenticator is mandatory, all the other cluster options are optional.

    Args:
        authenticator (Union[:class:`~.PasswordAuthenticator`, :class:`~.CertificateAuthenticator`]): An
            authenticator instance
        timeout_options (:class:`~.ClusterTimeoutOptions`): Timeout options for
            various SDK operations. See :class:`~.options.ClusterTimeoutOptions` for details.
        tracing_options (:class:`~.options.ClusterTimeoutOptions`): Tracing options for SDK tracing bevavior.
            See :class:`~.options.ClusterTracingOptions` for details.  These are ignored if an external tracer
            is specified.
        enable_tls (bool, optional): Set to True to enable tls. Defaults to False (disabled).
        enable_mutation_tokens (bool, optional): Set to False to disable mutation tokens in mutation results.
            Defaults to True (enabled).
        enable_tcp_keep_alive (bool, optional): Set to False to disable tcp keep alive. Defaults to True (enabled).
        ip_protocol (Union[str, :class:`.IpProtocol`): Set IP protocol. Defaults to IpProtocol.Any.
        enable_dns_srv (bool, optional): Set to False to disable DNS SRV. Defaults to True (enabled).
        show_queries (bool, optional): Set to True to enabled showing queries. Defaults to False (disabled).
        enable_unordered_execution (bool, optional): Set to False to disable unordered query execution.
            Defaults to True (enabled).
        enable_clustermap_notification (bool, optional): Set to False to disable cluster map notification.
            Defaults to True (enabled).
        enable_compression (bool, optional): Set to False to disable compression. Defaults to True (enabled).
        enable_tracing (bool, optional): Set to False to disable tracing (enables no-op tracer).
            Defaults to True (enabled).
        enable_metrics (bool, optional): Set to False to disable metrics (enables no-op meter).
            Defaults to True (enabled).
        network (str, optional): Set to False to disable compression. Defaults to True (enabled).
        tls_verify (Union[str, :class:`.TLSVerifyMode`], optional): Set tls verify mode. Defaults to
            TLSVerifyMode.PEER.
        serializer (:class:`~.serializer.Serializer`, optional): Global serializer to translate JSON to Python objects.
            Defaults to :class:`~.serializer.DefaultJsonSerializer`.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Global transcoder to use for kv-operations.
            Defaults to :class:`~.transcoder.JsonTranscoder`.
        tcp_keep_alive_interval (timedelta, optional): TCP keep-alive interval. Defaults to None.
        config_poll_interval (timedelta, optional): Config polling floor interval.
            Defaults to None.
        config_poll_floor (timedelta, optional): Config polling floor interval.
            Defaults to None.
        max_http_connections (int, optional): Maximum number of HTTP connections.  Defaults to None.
        logging_meter_emit_interval (timedelta, optional): Logging meter emit interval.  Defaults to 10 minutes.
        transaction_config (:class:`.TransactionConfig`, optional): Global configuration for transactions.
            Defaults to None.
        log_redaction (bool, optional): Set to True to enable log redaction. Defaults to False (disabled).
        compression (:class:`~.Compression`, optional): Set compression mode.  Defaults to None.
        compression_min_size (int, optional): Set compression min size.  Defaults to None.
        compression_min_ratio (float, optional): Set compression min size.  Defaults to None.
        compression (:class:`~.LockMode`, optional): Set LockMode mode.  Defaults to None.
        tracer (:class:`~couchbase.tracing.CouchbaseTracer`, optional): Set an external tracer.  Defaults to None,
            enabling the `threshold_logging_tracer`. Note when this is set, all tracing_options
            (see :class:`~.ClusterTracingOptions`) and then `enable_tracing` option are ignored.
        meter (:class:`~couchbase.metrics.CouchbaseMeter`, optional): Set an external meter.  Defaults to None,
            enabling the `logging_meter`.   Note when this is set, the `logging_meter_emit_interval` option is ignored.
        dns_nameserver (str, optional):  **VOLATILE** This API is subject to change at any time. Set to configure custom DNS nameserver. Defaults to None.
        dns_port (int, optional):  **VOLATILE** This API is subject to change at any time. Set to configure custom DNS port. Defaults to None.
    """  # noqa: E501

    def apply_profile(self,
                      profile_name  # type: Union[KnownConfigProfiles, str]
                      ) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided ConfigProfile options.

        Args:
            profile_name ([:class:`~couchbase.options.KnownConfigProfiles`, str]):  The name of the profile to apply
                toward ClusterOptions.
            authenticator (Union[:class:`~couchbase.auth.PasswordAuthenticator`, :class:`~couchbaes.auth.CertificateAuthenticator`]): An authenticator instance.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.

        """  # noqa: E501
        prof_name = profile_name.value if isinstance(profile_name, KnownConfigProfiles) else profile_name
        CONFIG_PROFILES.apply_profile(prof_name, self)

    @classmethod
    def create_options_with_profile(cls,
                                    authenticator,  # type: Optional[Authenticator]
                                    profile_name  # type: Union[KnownConfigProfiles, str]
                                    ) -> ClusterOptions:
        """
        **VOLATILE** This API is subject to change at any time.

        Create a ClusterOptions instance and apply the provided ConfigProfile options.

        Args:
            authenticator (Union[:class:`~couchbase.auth.PasswordAuthenticator`, :class:`~couchbaes.auth.CertificateAuthenticator`]): An authenticator instance.
            profile_name ([:class:`~couchbase.options.KnownConfigProfiles`, str]):  The name of the profile to apply
                toward ClusterOptions.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.

        """  # noqa: E501
        opts = cls(authenticator)
        prof_name = profile_name.value if isinstance(profile_name, KnownConfigProfiles) else profile_name
        CONFIG_PROFILES.apply_profile(prof_name, opts)
        return opts


"""

Python SDK Diagnostic Operation Options

"""

class DiagnosticsOptions(OptionsBase):
    """Available options to for a diagnostics operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        report_id (str, optional): A unique identifier for the report generated by this operation.
    """

class PingOptions(OptionsBase):
    """Available options to for a ping operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        report_id (str, optional): A unique identifier for the report generated by this operation.
        service_types (Iterable[class:`~couchbase.diagnostics.ServiceType`]): The services which should be pinged.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """

class WaitUntilReadyOptions(WaitUntilReadyOptionsBase):
    """Available options to for a wait until ready operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        desired_state (:class:`~.couchbase.diagnostics.ClusterState`, optional): The desired state to wait for in
            order to determine the cluster or bucket is ready.  Defaults to `Online`.
        service_types (Iterable[class:`~couchbase.diagnostics.ServiceType`]): The services which should be pinged.
    """

"""

Python SDK Key-Value Operation Options

"""

# Binary Operations

class AppendOptions(AppendOptionsBase):
    """Available options to for a binary append operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
    """

class DecrementOptions(DecrementOptionsBase):
    """Available options to for a decrement append operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        delta (:class:`.DeltaValue`, optional): The amount to increment the key. Defaults to 1.
        initial (:class:`.SignedInt64`, optional): The initial value to use for the document if it does not already
            exist. Defaults to 0.
    """

class IncrementOptions(IncrementOptionsBase):
    """Available options to for a binary increment operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        delta (:class:`.DeltaValue`, optional): The amount to increment the key. Defaults to 1.
        initial (:class:`.SignedInt64`, optional): The initial value to use for the document if it does not already
            exist. Defaults to 0.
    """

class PrependOptions(PrependOptionsBase):
    """Available options to for a binary prepend operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
    """

# Multi Operations

# Standard Operations

class ExistsOptions(ExistsOptionsBase):
    """Available options to for a key-value exists operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """

class GetOptions(GetOptionsBase):
    """Available options to for a key-value get operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        with_expiry (bool, optional): Indicates that the expiry of the document should be
            fetched alongside the data itself. Defaults to False.
        project (Iterable[str], optional): Specifies a list of fields within the document which should be fetched.
            This allows for easy retrieval of select fields without incurring the overhead of fetching the
            whole document.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """

class GetAllReplicasOptions(GetAllReplicasOptionsBase):
    """Available options to for a key-value get and touch operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class GetAndLockOptions(GetAndLockOptionsBase):
    """Available options to for a key-value get and lock operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class GetAndTouchOptions(GetAndTouchOptionsBase):
    """Available options to for a key-value get and touch operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class GetAnyReplicaOptions(GetAnyReplicaOptionsBase):
    """Available options to for a key-value get and touch operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class InsertOptions(InsertOptionsBase):
    """Available options to for a key-value insert operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (int, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class RemoveOptions(RemoveOptionsBase):
    """Available options to for a key-value remove operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
    """


class ReplaceOptions(ReplaceOptionsBase):
    """Available options to for a key-value replace operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (int, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class TouchOptions(TouchOptionsBase):
    """Available options to for a key-value exists operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """


class UnlockOptions(UnlockOptionsBase):
    """Available options to for a key-value exists operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """

class UpsertOptions(UpsertOptionsBase):
    """Available options to for a key-value upsert operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (int, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """

# Sub-document Operations

class LookupInOptions(LookupInOptionsBase):
    """Available options to for a subdocument lookup-in operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
    """


class MutateInOptions(MutateInOptionsBase):
    """Available options to for a subdocument mutate-in operation.

    .. warning::
        Importing options from ``couchbase.subdocument`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
        store_semantics (:class:`~couchbase.subdocument.StoreSemantics`, optional): Specifies the store semantics
            to use for this operation.
    """

"""

Python SDK Streaming Operation Options

"""

class QueryOptions(QueryOptionsBase):
    """Available options to for a N1QL (SQL++) query.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            query operation timeout.
        read_only (bool, optional): Specifies that this query should be executed in read-only mode,
            disabling the ability for the query to make any changes to the data. Defaults to False.
        scan_consistency (:class:`~couchbase.n1ql.QueryScanConsistency`, optional): Specifies the consistency
            requirements when executing the query.
        adhoc (bool, optional): Specifies whether this is an ad-hoc query, or if it should be prepared for
            faster execution in the future. Default to True.
        client_context_id (str, optional): The returned client context id for this query. Defaults to None.
        max_parallelism (int, optional): This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        positional_parameters (Iterable[JSONType], optional): Positional values to be used for the placeholders
            within the query. Defaults to None.
        named_parameters (Iterable[Dict[str, JSONType]], optional): Named values to be used for the placeholders
            within the query. Defaults to None.
        pipeline_batch (int, optional): This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        pipeline_cap (int, optional):  This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        profile (:class:`~couchbase.n1ql.QueryProfile`, optional): Specifies the level of profiling that should
            be used for the query. Defaults to `Off`.
        query_context (str, optional): Specifies the context within which this query should be executed. This can
            be scoped to a scope or a collection within the dataset. Defaults to None.
        scan_cap (int, optional):  This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        scan_wait (timedelta, optional):  This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        metrics (bool, optional): Specifies whether metrics should be captured as part of the execution of the query.
            Defaults to False.
        flex_index (bool, optional): Specifies whether flex-indexes should be enabled. Allowing the use of full-text
            search from the query service. Defaults to False.
        consistent_with (:class:`~couchbase.mutation_state.MutationState`, optional): Specifies a
            :class:`~couchbase.mutation_state.MutationState` which the query should be consistent with. Defaults to
            None.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer
            to use for this specific N1QL operation. Defaults to
            :class:`~couchbase.serializer.DefaultJsonSerializer`.
        raw (Dict[str, Any], optional): Specifies any additional parameters which should be passed to the query engine
            when executing the query. Defaults to None.
    """

"""

Couchbase Python SDK constrained integer classes

"""


class ConstrainedInt(ConstrainedIntBase):
    pass


class SignedInt64(SignedInt64Base):
    pass


class UnsignedInt32(UnsignedInt32Base):
    pass


class UnsignedInt64(UnsignedInt64Base):
    pass


class DeltaValue(DeltaValueBase):
    pass