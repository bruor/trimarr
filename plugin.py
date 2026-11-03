import copy
import logging
import random
import sys
import threading
from contextlib import contextmanager
from functools import wraps
from typing import Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Module-level pointer to the active plugin instance
ACTIVE_PLUGIN = None

# Thread-local state to scope QuerySet/profile reordering strictly to stream selection calls
_SELECTION_CTX = threading.local()

# Default settings fallback
DEFAULT_SETTINGS = {
    "enabled": True,
    "tie_breaker": "default_first",
}


def _in_selection_context() -> bool:
    return getattr(_SELECTION_CTX, "depth", 0) > 0


@contextmanager
def _profile_selection_context():
    _SELECTION_CTX.depth = getattr(_SELECTION_CTX, "depth", 0) + 1
    try:
        yield
    finally:
        _SELECTION_CTX.depth = max(0, getattr(_SELECTION_CTX, "depth", 1) - 1)


@contextmanager
def _pause_selection_context():
    prev = getattr(_SELECTION_CTX, "depth", 0)
    _SELECTION_CTX.depth = 0
    try:
        yield
    finally:
        _SELECTION_CTX.depth = prev


def _reorder_profiles_for_default_walk(profiles_iterable) -> list:
    """
    Reorders an iterable of M3UAccountProfile instances for Dispatcharr callers that execute:
        default_profile = next((obj for obj in m3u_profiles if obj.is_default), None)
        profiles = [default_profile] + [obj for obj in m3u_profiles if not obj.is_default]

    By sorting active profiles in least-connections order and setting `is_default = True`
    on a shallow copy of the first (least-used) active profile and `is_default = False`
    on shallow copies of the remaining profiles, Dispatcharr's own list comprehension
    produces the exact least-connections walk order without mutating ORM cache instances.
    """
    items = list(profiles_iterable)
    if len(items) <= 1 or not ACTIVE_PLUGIN or not ACTIVE_PLUGIN.is_enabled():
        return items

    with _pause_selection_context():
        active_items = [p for p in items if getattr(p, "is_active", True)]
        inactive_items = [p for p in items if not getattr(p, "is_active", True)]

        # Dispatcharr requires an active default profile on the account; if none exists,
        # preserve that condition so callers still detect `default_profile is None`.
        has_active_default = any(getattr(p, "is_default", False) for p in active_items)
        if not has_active_default or not active_items:
            return items

        sorted_active = ACTIVE_PLUGIN.sort_profiles_least_used(active_items)

        result = []
        for idx, prof in enumerate(sorted_active):
            prof_copy = copy.copy(prof)
            prof_copy._trimarr_orig_is_default = getattr(prof, "is_default", False)
            prof_copy.is_default = idx == 0
            result.append(prof_copy)

        for prof in inactive_items:
            prof_copy = copy.copy(prof)
            prof_copy._trimarr_orig_is_default = getattr(prof, "is_default", False)
            prof_copy.is_default = False
            result.append(prof_copy)

        return result


class Plugin:
    name = "Trimarr"
    version = "1.0.0"
    description = (
        "Load-balances provider connections across all active M3U account profiles "
        "using least-connections selection instead of linear failover."
    )
    author = "bruor"

    fields = [
        {
            "id": "enabled",
            "label": "Enable Trimarr Load Balancer",
            "type": "boolean",
            "default": True,
            "help_text": "Master switch to enable least-connections load balancing across M3U account profiles instead of Dispatcharr's default linear failover.",
            "description": "Master switch to enable least-connections load balancing across M3U account profiles instead of Dispatcharr's default linear failover.",
        },
        {
            "id": "tie_breaker",
            "label": "Tie-Breaker Strategy",
            "type": "select",
            "default": "default_first",
            "options": [
                {
                    "value": "default_first",
                    "label": "Default Profile First (Deterministic)",
                },
                {
                    "value": "utilization",
                    "label": "Lowest Capacity Utilization Ratio (%)",
                },
                {
                    "value": "random",
                    "label": "Random (Even Wear Across Tied Profiles)",
                },
            ],
            "help_text": "How Trimarr breaks ties when multiple active profiles on a provider have the exact same lowest number of active connections.",
            "description": "How Trimarr breaks ties when multiple active profiles on a provider have the exact same lowest number of active connections.",
        },
    ]

    actions = [
        {
            "id": "status_check",
            "label": "Check Hook Status",
            "description": "Verify Trimarr load balancer status, active settings, and runtime hook state across Dispatcharr modules.",
            "button_label": "Check Status",
            "button_variant": "filled",
            "button_color": "blue",
        },
        {
            "id": "inspect_profiles",
            "label": "Inspect Profile Load",
            "description": "View live Redis connection counts across all active M3U accounts and profiles and see which profile will be selected next.",
            "button_label": "Inspect Load",
            "button_variant": "outline",
            "button_color": "green",
        },
    ]

    def __init__(self):
        global ACTIVE_PLUGIN
        ACTIVE_PLUGIN = self
        self._cached_settings = {}
        self._inject_hooks()
        logger.info("[Trimarr] Plugin initialized and registered as active instance.")

    @staticmethod
    def _import_module(module_path: str):
        mod = sys.modules.get(module_path)
        if mod is None:
            try:
                __import__(module_path)
                mod = sys.modules.get(module_path)
            except ImportError:
                return None
        return mod

    @staticmethod
    def _get_redis_client():
        try:
            from core.utils import RedisClient

            return RedisClient.get_client()
        except Exception:
            return None

    def _get_plugin_config(self):
        """
        Look up the persisted PluginConfig row from Dispatcharr's database.
        Matches by module key (if loaded as `_dispatcharr_plugin_<key>.plugin` or `plugins.<key>`),
        `key='trimarr'`, or `name='Trimarr'`.
        """
        try:
            from apps.plugins.models import PluginConfig

            if __name__.startswith("_dispatcharr_plugin_"):
                runtime_key = __name__[len("_dispatcharr_plugin_") :].split(".", 1)[0]
                config = PluginConfig.objects.filter(key=runtime_key).first()
                if config is not None:
                    return config

            if __name__.startswith("plugins."):
                runtime_key = __name__.split(".", 1)[1]
                config = PluginConfig.objects.filter(key=runtime_key).first()
                if config is not None:
                    return config

            config = PluginConfig.objects.filter(key__iexact="trimarr").first()
            if config is not None:
                return config

            return PluginConfig.objects.filter(name__iexact="trimarr").first()
        except Exception:
            return None

    def get_setting(self, key: str, default=None):
        """
        Retrieves a plugin setting.
        Always queries Dispatcharr's persisted PluginConfig first so settings updates
        made in the UI take effect immediately across all uWSGI worker processes.
        Falls back to in-memory `_cached_settings` and then `DEFAULT_SETTINGS`.
        """
        config = self._get_plugin_config()
        if config is not None and isinstance(config.settings, dict):
            val = config.settings.get(key)
            if val is not None:
                return val

        if key in self._cached_settings:
            return self._cached_settings[key]

        return DEFAULT_SETTINGS.get(key, default)

    def is_enabled(self) -> bool:
        """
        Returns True if the plugin is active both in Dispatcharr's DB model and in plugin settings.
        """
        config = self._get_plugin_config()
        if config is not None and not config.enabled:
            return False

        val = self.get_setting("enabled", True)
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.lower() in ("true", "1", "yes")
        return bool(val)

    def get_profile_load_info(self, profile, redis_client=None) -> Tuple[int, bool]:
        """
        Returns `(effective_connections, has_capacity)` for an M3UAccountProfile.
        Accounts for both the per-profile Redis counter (`profile_connections:{id}`)
        and any shared ServerGroup credential counter (`server_group_connections:{group}:{fp}`).
        """
        if redis_client is None:
            redis_client = self._get_redis_client()

        max_streams = getattr(profile, "max_streams", 0) or 0
        if redis_client is None:
            return 0, True

        conn_pool = self._import_module("apps.m3u.connection_pool")
        if conn_pool is not None:
            try:
                prof_count = int(conn_pool.get_profile_connection_count(profile, redis_client) or 0)
            except Exception:
                prof_count = int(redis_client.get(f"profile_connections:{profile.id}") or 0)

            try:
                cred_count = int(conn_pool.get_credential_connection_count(profile, redis_client) or 0)
            except Exception:
                cred_count = 0

            try:
                has_cap = bool(conn_pool.pool_has_capacity_for_profile(profile, redis_client))
            except Exception:
                has_cap = True if max_streams == 0 else (prof_count < max_streams)

            return max(prof_count, cred_count), has_cap

        # Fallback if connection_pool module is unavailable
        prof_count = int(redis_client.get(f"profile_connections:{profile.id}") or 0)
        has_cap = True if max_streams == 0 else (prof_count < max_streams)
        return prof_count, has_cap

    def sort_profiles_least_used(self, profiles: Iterable, redis_client=None) -> List:
        """
        Sorts candidate M3UAccountProfile objects so that:
        1. Profiles with available capacity come before profiles at capacity.
        2. Profiles with fewer active connections come first (least-connections load balancing).
        3. Ties are broken according to the configured `tie_breaker` setting:
           - `default_first`: Default profile first, then ascending profile ID.
           - `utilization`: Lowest `active / max_streams` ratio first, then default profile, then ID.
           - `random`: Random order among tied profiles.
        """
        profile_list = list(profiles)
        if len(profile_list) <= 1:
            return profile_list

        if redis_client is None:
            redis_client = self._get_redis_client()
        if redis_client is None:
            return profile_list

        tie_breaker = str(self.get_setting("tie_breaker", "default_first") or "default_first").strip().lower()

        def sort_key(prof):
            effective_count, has_capacity = self.get_profile_load_info(prof, redis_client)
            orig_default = getattr(
                prof, "_trimarr_orig_is_default", getattr(prof, "is_default", False)
            )
            prof_id = getattr(prof, "id", 0) or 0
            max_streams = getattr(prof, "max_streams", 0) or 0

            if tie_breaker == "random":
                return (not has_capacity, effective_count, random.random(), prof_id)
            if tie_breaker == "utilization":
                utilization = (effective_count / max_streams) if max_streams > 0 else 0.0
                return (not has_capacity, effective_count, utilization, not orig_default, prof_id)
            # default_first
            return (not has_capacity, effective_count, not orig_default, prof_id)

        return sorted(profile_list, key=sort_key)

    def _inject_hooks(self):
        """
        Installs runtime wrappers across Dispatcharr's profile selection modules.
        """
        self._hook_queryset_iter()
        self._hook_channels_models()
        self._hook_live_proxy_url_utils()
        self._hook_vod_proxy_views()
        self._hook_timeshift_views()
        self._hook_m3u_utils()

    def _hook_queryset_iter(self):
        qs_mod = self._import_module("django.db.models.query")
        if qs_mod is None or not hasattr(qs_mod, "QuerySet"):
            return

        QuerySet = qs_mod.QuerySet
        if not hasattr(QuerySet, "_trimarr_orig_iter"):
            QuerySet._trimarr_orig_iter = QuerySet.__iter__

        orig_iter = QuerySet._trimarr_orig_iter

        @wraps(orig_iter)
        def patched_iter(qs_self):
            if not _in_selection_context() or not ACTIVE_PLUGIN or not ACTIVE_PLUGIN.is_enabled():
                return orig_iter(qs_self)

            model = getattr(qs_self, "model", None)
            model_name = getattr(model, "__name__", "")
            if model_name != "M3UAccountProfile":
                return orig_iter(qs_self)

            # Cache the reordered list on this specific QuerySet instance so two back-to-back
            # iterations of `m3u_profiles` (finding `default_profile` then `[not is_default]`)
            # always see the exact same order, even when `tie_breaker="random"` is active.
            cached = getattr(qs_self, "_trimarr_reordered_cache", None)
            if cached is not None:
                return iter(cached)

            raw_items = list(orig_iter(qs_self))
            reordered = _reorder_profiles_for_default_walk(raw_items)
            try:
                qs_self._trimarr_reordered_cache = reordered
            except Exception:
                pass
            return iter(reordered)

        patched_iter._is_trimarr_patched = True
        QuerySet.__iter__ = patched_iter

    def _hook_channels_models(self):
        channels_mod = self._import_module("apps.channels.models")
        if channels_mod is None:
            return

        # Hook Channel.get_stream
        Channel = getattr(channels_mod, "Channel", None)
        if Channel is not None and hasattr(Channel, "get_stream"):
            if not hasattr(Channel, "_trimarr_orig_get_stream"):
                Channel._trimarr_orig_get_stream = Channel.get_stream

            orig_channel_get_stream = Channel._trimarr_orig_get_stream

            @wraps(orig_channel_get_stream)
            def patched_channel_get_stream(channel_self, *args, **kwargs):
                if not ACTIVE_PLUGIN or not ACTIVE_PLUGIN.is_enabled():
                    return orig_channel_get_stream(channel_self, *args, **kwargs)
                with _profile_selection_context():
                    return orig_channel_get_stream(channel_self, *args, **kwargs)

            patched_channel_get_stream._is_trimarr_patched = True
            Channel.get_stream = patched_channel_get_stream

        # Hook Stream.get_stream
        Stream = getattr(channels_mod, "Stream", None)
        if Stream is not None and hasattr(Stream, "get_stream"):
            if not hasattr(Stream, "_trimarr_orig_get_stream"):
                Stream._trimarr_orig_get_stream = Stream.get_stream

            orig_stream_get_stream = Stream._trimarr_orig_get_stream

            @wraps(orig_stream_get_stream)
            def patched_stream_get_stream(stream_self, preferred_profile_id=None, *args, **kwargs):
                if (
                    not ACTIVE_PLUGIN
                    or not ACTIVE_PLUGIN.is_enabled()
                    or preferred_profile_id is not None
                ):
                    return orig_stream_get_stream(
                        stream_self, preferred_profile_id=preferred_profile_id, *args, **kwargs
                    )
                with _profile_selection_context():
                    return orig_stream_get_stream(
                        stream_self, preferred_profile_id=preferred_profile_id, *args, **kwargs
                    )

            patched_stream_get_stream._is_trimarr_patched = True
            Stream.get_stream = patched_stream_get_stream

    def _hook_live_proxy_url_utils(self):
        url_utils = self._import_module("apps.proxy.live_proxy.url_utils")
        if url_utils is None:
            return

        for fn_name in (
            "pick_stream_m3u_profile_id",
            "get_stream_info_for_switch",
            "get_alternate_streams",
        ):
            if not hasattr(url_utils, fn_name):
                continue
            orig_attr = f"_trimarr_orig_{fn_name}"
            if not hasattr(url_utils, orig_attr):
                setattr(url_utils, orig_attr, getattr(url_utils, fn_name))

            orig_fn = getattr(url_utils, orig_attr)

            def _make_wrapper(fn):
                @wraps(fn)
                def wrapped(*args, **kwargs):
                    if not ACTIVE_PLUGIN or not ACTIVE_PLUGIN.is_enabled():
                        return fn(*args, **kwargs)
                    with _profile_selection_context():
                        return fn(*args, **kwargs)

                wrapped._is_trimarr_patched = True
                return wrapped

            setattr(url_utils, fn_name, _make_wrapper(orig_fn))

    def _hook_vod_proxy_views(self):
        vod_views = self._import_module("apps.proxy.vod_proxy.views")
        if vod_views is None or not hasattr(vod_views, "_get_m3u_profile"):
            return

        if not hasattr(vod_views, "_trimarr_orig_get_m3u_profile"):
            vod_views._trimarr_orig_get_m3u_profile = vod_views._get_m3u_profile

        orig_get_m3u_profile = vod_views._trimarr_orig_get_m3u_profile

        @wraps(orig_get_m3u_profile)
        def patched_get_m3u_profile(
            m3u_account, profile_id, session_id=None, restrict_to_profile_ids=None
        ):
            if (
                not ACTIVE_PLUGIN
                or not ACTIVE_PLUGIN.is_enabled()
                or profile_id is not None
                or m3u_account is None
            ):
                return orig_get_m3u_profile(
                    m3u_account,
                    profile_id,
                    session_id=session_id,
                    restrict_to_profile_ids=restrict_to_profile_ids,
                )

            redis_client = ACTIVE_PLUGIN._get_redis_client()
            if redis_client is None:
                return orig_get_m3u_profile(
                    m3u_account,
                    profile_id,
                    session_id=session_id,
                    restrict_to_profile_ids=restrict_to_profile_ids,
                )

            # If session_id already has an active persistent VOD connection, let
            # the original function reuse it so HTTP Range requests stay sticky.
            if session_id:
                try:
                    conn_data = redis_client.hgetall(f"vod_persistent_connection:{session_id}")
                    if conn_data and (
                        conn_data.get("m3u_profile_id") or conn_data.get(b"m3u_profile_id")
                    ):
                        return orig_get_m3u_profile(
                            m3u_account,
                            profile_id,
                            session_id=session_id,
                            restrict_to_profile_ids=restrict_to_profile_ids,
                        )
                except Exception:
                    pass

            try:
                M3UAccountProfile = getattr(vod_views, "M3UAccountProfile", None)
                if M3UAccountProfile is not None and hasattr(M3UAccountProfile, "objects"):
                    qs = M3UAccountProfile.objects.filter(
                        m3u_account=m3u_account, is_active=True
                    ).select_related("m3u_account__user_agent")
                    if restrict_to_profile_ids is not None:
                        qs = qs.filter(id__in=restrict_to_profile_ids)
                    candidates = list(qs)
                else:
                    candidates = [
                        p
                        for p in m3u_account.profiles.filter(is_active=True)
                        if restrict_to_profile_ids is None or p.id in restrict_to_profile_ids
                    ]

                has_default = any(getattr(p, "is_default", False) for p in candidates)
                if not has_default and restrict_to_profile_ids is None:
                    # Match Dispatcharr's requirement that unrestricted accounts have a default profile
                    return orig_get_m3u_profile(
                        m3u_account,
                        profile_id,
                        session_id=session_id,
                        restrict_to_profile_ids=restrict_to_profile_ids,
                    )

                sorted_candidates = ACTIVE_PLUGIN.sort_profiles_least_used(
                    candidates, redis_client
                )
                for candidate in sorted_candidates:
                    _count, has_cap = ACTIVE_PLUGIN.get_profile_load_info(candidate, redis_client)
                    if has_cap:
                        result = orig_get_m3u_profile(
                            m3u_account,
                            candidate.id,
                            session_id=session_id,
                            restrict_to_profile_ids=restrict_to_profile_ids,
                        )
                        if result and result[0]:
                            return result
            except Exception as exc:
                logger.warning(
                    "[Trimarr] Error selecting least-used VOD profile, falling back: %s", exc
                )

            return orig_get_m3u_profile(
                m3u_account,
                profile_id,
                session_id=session_id,
                restrict_to_profile_ids=restrict_to_profile_ids,
            )

        patched_get_m3u_profile._is_trimarr_patched = True
        vod_views._get_m3u_profile = patched_get_m3u_profile

    def _hook_timeshift_views(self):
        ts_views = self._import_module("apps.timeshift.views")
        if ts_views is None or not hasattr(ts_views, "_prepare_catchup_stream_attempt"):
            return

        if not hasattr(ts_views, "_trimarr_orig_prepare_catchup_stream_attempt"):
            ts_views._trimarr_orig_prepare_catchup_stream_attempt = (
                ts_views._prepare_catchup_stream_attempt
            )

        orig_prepare = ts_views._trimarr_orig_prepare_catchup_stream_attempt

        @wraps(orig_prepare)
        def patched_prepare_catchup_stream_attempt(
            catchup_stream, *, timestamp, redis_client, reserve, profile=None
        ):
            if (
                not ACTIVE_PLUGIN
                or not ACTIVE_PLUGIN.is_enabled()
                or profile is not None
                or redis_client is None
            ):
                return orig_prepare(
                    catchup_stream,
                    timestamp=timestamp,
                    redis_client=redis_client,
                    reserve=reserve,
                    profile=profile,
                )

            m3u_account = getattr(catchup_stream, "m3u_account", None)
            if m3u_account is None or getattr(m3u_account, "account_type", None) != "XC":
                return orig_prepare(
                    catchup_stream,
                    timestamp=timestamp,
                    redis_client=redis_client,
                    reserve=reserve,
                    profile=profile,
                )

            try:
                m3u_profiles = list(m3u_account.profiles.filter(is_active=True))
                default_profile = next(
                    (p for p in m3u_profiles if getattr(p, "is_default", False)), None
                )
                if default_profile is None:
                    return orig_prepare(
                        catchup_stream,
                        timestamp=timestamp,
                        redis_client=redis_client,
                        reserve=reserve,
                        profile=profile,
                    )

                sorted_profiles = ACTIVE_PLUGIN.sort_profiles_least_used(
                    m3u_profiles, redis_client
                )
                any_capacity_blocked = False
                for candidate in sorted_profiles:
                    target, capacity_blocked = orig_prepare(
                        catchup_stream,
                        timestamp=timestamp,
                        redis_client=redis_client,
                        reserve=reserve,
                        profile=candidate,
                    )
                    if target is not None:
                        return target, False
                    if capacity_blocked:
                        any_capacity_blocked = True

                return None, any_capacity_blocked
            except Exception as exc:
                logger.warning(
                    "[Trimarr] Error in timeshift least-used profile walk, falling back: %s",
                    exc,
                )
                return orig_prepare(
                    catchup_stream,
                    timestamp=timestamp,
                    redis_client=redis_client,
                    reserve=reserve,
                    profile=profile,
                )

        patched_prepare_catchup_stream_attempt._is_trimarr_patched = True
        ts_views._prepare_catchup_stream_attempt = patched_prepare_catchup_stream_attempt

    def _hook_m3u_utils(self):
        m3u_utils = self._import_module("apps.m3u.utils")
        if m3u_utils is None or not hasattr(m3u_utils, "get_allowed_m3u_profiles"):
            return

        if not hasattr(m3u_utils, "_trimarr_orig_get_allowed_m3u_profiles"):
            m3u_utils._trimarr_orig_get_allowed_m3u_profiles = (
                m3u_utils.get_allowed_m3u_profiles
            )

        orig_get_allowed = m3u_utils._trimarr_orig_get_allowed_m3u_profiles

        @wraps(orig_get_allowed)
        def patched_get_allowed_m3u_profiles(user):
            profiles_by_account = orig_get_allowed(user)
            if (
                not ACTIVE_PLUGIN
                or not ACTIVE_PLUGIN.is_enabled()
                or not isinstance(profiles_by_account, dict)
                or not profiles_by_account
            ):
                return profiles_by_account

            try:
                redis_client = ACTIVE_PLUGIN._get_redis_client()
                if redis_client is not None:
                    for account_id, prof_list in list(profiles_by_account.items()):
                        if len(prof_list) > 1:
                            profiles_by_account[account_id] = (
                                ACTIVE_PLUGIN.sort_profiles_least_used(
                                    prof_list, redis_client
                                )
                            )
            except Exception as exc:
                logger.debug("[Trimarr] Error sorting allowed M3U profiles: %s", exc)

            return profiles_by_account

        patched_get_allowed_m3u_profiles._is_trimarr_patched = True
        m3u_utils.get_allowed_m3u_profiles = patched_get_allowed_m3u_profiles

    def stop(self, context=None):
        """
        Called by Dispatcharr's PluginManager when the plugin is disabled, reloaded, or deleted.
        Restores all original Dispatcharr functions/methods and clears ACTIVE_PLUGIN.
        """
        global ACTIVE_PLUGIN

        qs_mod = sys.modules.get("django.db.models.query")
        if qs_mod is not None and hasattr(qs_mod, "QuerySet"):
            QuerySet = qs_mod.QuerySet
            if hasattr(QuerySet, "_trimarr_orig_iter"):
                QuerySet.__iter__ = QuerySet._trimarr_orig_iter

        channels_mod = sys.modules.get("apps.channels.models")
        if channels_mod is not None:
            Channel = getattr(channels_mod, "Channel", None)
            if Channel is not None and hasattr(Channel, "_trimarr_orig_get_stream"):
                Channel.get_stream = Channel._trimarr_orig_get_stream
            Stream = getattr(channels_mod, "Stream", None)
            if Stream is not None and hasattr(Stream, "_trimarr_orig_get_stream"):
                Stream.get_stream = Stream._trimarr_orig_get_stream

        url_utils = sys.modules.get("apps.proxy.live_proxy.url_utils")
        if url_utils is not None:
            for fn_name in (
                "pick_stream_m3u_profile_id",
                "get_stream_info_for_switch",
                "get_alternate_streams",
            ):
                orig_attr = f"_trimarr_orig_{fn_name}"
                if hasattr(url_utils, orig_attr):
                    setattr(url_utils, fn_name, getattr(url_utils, orig_attr))

        vod_views = sys.modules.get("apps.proxy.vod_proxy.views")
        if vod_views is not None and hasattr(vod_views, "_trimarr_orig_get_m3u_profile"):
            vod_views._get_m3u_profile = vod_views._trimarr_orig_get_m3u_profile

        ts_views = sys.modules.get("apps.timeshift.views")
        if ts_views is not None and hasattr(
            ts_views, "_trimarr_orig_prepare_catchup_stream_attempt"
        ):
            ts_views._prepare_catchup_stream_attempt = (
                ts_views._trimarr_orig_prepare_catchup_stream_attempt
            )

        m3u_utils = sys.modules.get("apps.m3u.utils")
        if m3u_utils is not None and hasattr(
            m3u_utils, "_trimarr_orig_get_allowed_m3u_profiles"
        ):
            m3u_utils.get_allowed_m3u_profiles = (
                m3u_utils._trimarr_orig_get_allowed_m3u_profiles
            )

        if ACTIVE_PLUGIN is self:
            ACTIVE_PLUGIN = None

        logger.info("[Trimarr] Plugin stopped and original hooks restored (context=%s).", context)

    def _get_hook_states(self) -> dict:
        states = {}

        qs_mod = sys.modules.get("django.db.models.query")
        states["queryset_iter"] = bool(
            qs_mod is not None
            and hasattr(qs_mod, "QuerySet")
            and getattr(qs_mod.QuerySet.__iter__, "_is_trimarr_patched", False)
        )

        channels_mod = sys.modules.get("apps.channels.models")
        states["channels"] = bool(
            channels_mod is not None
            and hasattr(channels_mod, "Channel")
            and getattr(channels_mod.Channel.get_stream, "_is_trimarr_patched", False)
        )

        url_utils = sys.modules.get("apps.proxy.live_proxy.url_utils")
        states["live_proxy"] = bool(
            url_utils is not None
            and hasattr(url_utils, "pick_stream_m3u_profile_id")
            and getattr(url_utils.pick_stream_m3u_profile_id, "_is_trimarr_patched", False)
        )

        vod_views = sys.modules.get("apps.proxy.vod_proxy.views")
        states["vod_proxy"] = bool(
            vod_views is not None
            and hasattr(vod_views, "_get_m3u_profile")
            and getattr(vod_views._get_m3u_profile, "_is_trimarr_patched", False)
        )

        ts_views = sys.modules.get("apps.timeshift.views")
        states["timeshift"] = bool(
            ts_views is not None
            and hasattr(ts_views, "_prepare_catchup_stream_attempt")
            and getattr(
                ts_views._prepare_catchup_stream_attempt, "_is_trimarr_patched", False
            )
        )

        m3u_utils = sys.modules.get("apps.m3u.utils")
        states["m3u_utils"] = bool(
            m3u_utils is not None
            and hasattr(m3u_utils, "get_allowed_m3u_profiles")
            and getattr(m3u_utils.get_allowed_m3u_profiles, "_is_trimarr_patched", False)
        )

        return states

    def run(self, action: str, params: dict = None, context: dict = None) -> dict:
        """
        Handles synchronous administrative actions triggered from the Dispatcharr UI.
        """
        if context and isinstance(context.get("settings"), dict):
            self._cached_settings.update(context["settings"])

        if action == "status_check":
            hook_states = self._get_hook_states()
            any_hooked = any(hook_states.values())
            enabled = self.is_enabled()
            tie_breaker = self.get_setting("tie_breaker", "default_first")

            hook_summary = ", ".join(
                f"{k}={'ok' if v else 'unhooked'}" for k, v in hook_states.items()
            )
            return {
                "status": "ok",
                "is_enabled": enabled,
                "is_hooked": any_hooked,
                "hook_states": hook_states,
                "tie_breaker": tie_breaker,
                "message": (
                    f"Trimarr is {'ACTIVE' if (enabled and any_hooked) else 'DISABLED or unhooked'} "
                    f"(tie_breaker={tie_breaker}). "
                    f"Hooks: [{hook_summary}]"
                ),
            }

        if action == "inspect_profiles":
            try:
                from apps.m3u.models import M3UAccount

                redis_client = self._get_redis_client()
                accounts = M3UAccount.objects.filter(is_active=True).prefetch_related(
                    "profiles"
                )
                account_summaries = []
                lines = []

                for account in accounts:
                    active_profiles = [
                        p for p in account.profiles.all() if getattr(p, "is_active", True)
                    ]
                    if not active_profiles:
                        continue

                    sorted_profiles = self.sort_profiles_least_used(
                        active_profiles, redis_client
                    )
                    next_profile = None
                    prof_details = []

                    for p in sorted_profiles:
                        count, has_cap = self.get_profile_load_info(p, redis_client)
                        if next_profile is None and has_cap:
                            next_profile = p
                        max_label = "unlimited" if (p.max_streams or 0) == 0 else str(p.max_streams)
                        prof_details.append(
                            {
                                "id": p.id,
                                "name": p.name,
                                "is_default": bool(p.is_default),
                                "active_connections": count,
                                "max_streams": p.max_streams,
                                "has_capacity": has_cap,
                            }
                        )

                    next_label = (
                        f"{next_profile.name} (id={next_profile.id})"
                        if next_profile
                        else "NONE (all at capacity)"
                    )
                    prof_str = ", ".join(
                        f"{d['name']}: {d['active_connections']}/{'inf' if d['max_streams'] == 0 else d['max_streams']}"
                        for d in prof_details
                    )
                    lines.append(f"{account.name} -> Next: {next_label} [{prof_str}]")
                    account_summaries.append(
                        {
                            "account_id": account.id,
                            "account_name": account.name,
                            "next_profile_id": next_profile.id if next_profile else None,
                            "next_profile_name": next_profile.name if next_profile else None,
                            "profiles": prof_details,
                        }
                    )

                msg = (
                    " | ".join(lines)
                    if lines
                    else "No active M3U accounts with active profiles found."
                )
                return {
                    "status": "ok",
                    "accounts": account_summaries,
                    "message": msg,
                }
            except Exception as exc:
                return {
                    "status": "error",
                    "error": f"Failed to inspect profiles: {exc}",
                    "message": f"Failed to inspect profiles: {exc}",
                }

        return {
            "status": "error",
            "error": f"Unknown action: {action}",
            "message": f"Unknown action: {action}",
        }
