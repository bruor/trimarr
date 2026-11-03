import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock


class FakeRedis:
    def __init__(self):
        self.store = {}
        self.hashes = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, val):
        self.store[key] = str(val)

    def incr(self, key):
        val = int(self.store.get(key, 0) or 0) + 1
        self.store[key] = str(val)
        return val

    def decr(self, key):
        val = int(self.store.get(key, 0) or 0) - 1
        self.store[key] = str(val)
        return val

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, field, val):
        self.hashes.setdefault(key, {})[field] = val


class FakeProfileQuerySet:
    """Simulates Django's QuerySet for M3UAccountProfile."""

    class M3UAccountProfile:
        pass

    model = M3UAccountProfile

    def __init__(self, items):
        self._items = list(items)

    def __iter__(self):
        return iter(self._items)

    def filter(self, **kwargs):
        filtered = list(self._items)
        if "is_active" in kwargs:
            filtered = [p for p in filtered if p.is_active == kwargs["is_active"]]
        if "is_default" in kwargs:
            filtered = [p for p in filtered if p.is_default == kwargs["is_default"]]
        if "id" in kwargs:
            filtered = [p for p in filtered if p.id == kwargs["id"]]
        if "id__in" in kwargs:
            allowed = set(kwargs["id__in"])
            filtered = [p for p in filtered if p.id in allowed]
        return FakeProfileQuerySet(filtered)

    def select_related(self, *args, **kwargs):
        return self

    def first(self):
        return self._items[0] if self._items else None

    def get(self, **kwargs):
        res = self.filter(**kwargs).first()
        if res is None:
            raise Exception("DoesNotExist")
        return res

    def all(self):
        return FakeProfileQuerySet(self._items)


class TestTrimarrPlugin(unittest.TestCase):
    def setUp(self):
        self.fake_redis = FakeRedis()
        self.cred_counts = {}

        # 1. Mock core.utils.RedisClient
        mock_core_utils = types.ModuleType("core.utils")
        mock_redis_cls = MagicMock()
        mock_redis_cls.get_client.return_value = self.fake_redis
        mock_core_utils.RedisClient = mock_redis_cls
        sys.modules["core"] = types.ModuleType("core")
        sys.modules["core.utils"] = mock_core_utils

        # 2. Mock django.db.models.query.QuerySet
        mock_django_query = types.ModuleType("django.db.models.query")
        mock_django_query.QuerySet = FakeProfileQuerySet
        sys.modules["django"] = types.ModuleType("django")
        sys.modules["django.db"] = types.ModuleType("django.db")
        sys.modules["django.db.models"] = types.ModuleType("django.db.models")
        sys.modules["django.db.models.query"] = mock_django_query

        # 3. Mock apps.m3u.connection_pool
        mock_conn_pool = types.ModuleType("apps.m3u.connection_pool")

        def profile_connections_key(profile_id: int) -> str:
            return f"profile_connections:{profile_id}"

        def get_profile_connection_count(profile, redis_client) -> int:
            return int(redis_client.get(profile_connections_key(profile.id)) or 0)

        def get_credential_connection_count(profile, redis_client) -> int:
            return int(self.cred_counts.get(profile.id, 0))

        def profile_has_capacity_for_selection(profile, redis_client) -> bool:
            if profile.max_streams == 0:
                return True
            return get_profile_connection_count(profile, redis_client) < profile.max_streams

        def group_has_capacity_for_profile(profile, redis_client) -> bool:
            if profile.max_streams == 0:
                return True
            cred_c = get_credential_connection_count(profile, redis_client)
            return cred_c < profile.max_streams

        def pool_has_capacity_for_profile(profile, redis_client) -> bool:
            return profile_has_capacity_for_selection(
                profile, redis_client
            ) and group_has_capacity_for_profile(profile, redis_client)

        def reserve_profile_slot(profile, redis_client):
            key = profile_connections_key(profile.id)
            count = 0
            if profile.max_streams > 0:
                count = redis_client.incr(key)
                if count > profile.max_streams:
                    redis_client.decr(key)
                    return False, count - 1, "profile_full"
            if not group_has_capacity_for_profile(profile, redis_client):
                if profile.max_streams > 0:
                    redis_client.decr(key)
                return False, count - 1 if profile.max_streams > 0 else 0, "credential_full"
            return True, count, None

        mock_conn_pool.profile_connections_key = profile_connections_key
        mock_conn_pool.get_profile_connection_count = get_profile_connection_count
        mock_conn_pool.get_credential_connection_count = get_credential_connection_count
        mock_conn_pool.pool_has_capacity_for_profile = pool_has_capacity_for_profile
        mock_conn_pool.reserve_profile_slot = reserve_profile_slot
        sys.modules["apps"] = types.ModuleType("apps")
        sys.modules["apps.m3u"] = types.ModuleType("apps.m3u")
        sys.modules["apps.m3u.connection_pool"] = mock_conn_pool

        # 4. Create test M3UAccount & 3 profiles (cred1 default, cred2, cred3)
        self.account = SimpleNamespace(
            id=10,
            name="Provider Alpha",
            is_active=True,
            account_type="XC",
        )
        self.prof1 = SimpleNamespace(
            id=101,
            name="cred1_default",
            is_default=True,
            is_active=True,
            max_streams=3,
            m3u_account=self.account,
            m3u_account_id=10,
        )
        self.prof2 = SimpleNamespace(
            id=102,
            name="cred2",
            is_default=False,
            is_active=True,
            max_streams=3,
            m3u_account=self.account,
            m3u_account_id=10,
        )
        self.prof3 = SimpleNamespace(
            id=103,
            name="cred3",
            is_default=False,
            is_active=True,
            max_streams=3,
            m3u_account=self.account,
            m3u_account_id=10,
        )
        self.account.profiles = FakeProfileQuerySet([self.prof1, self.prof2, self.prof3])

        # 5. Mock apps.channels.models (Channel & Stream using Dispatcharr's exact profile walk)
        mock_channels_models = types.ModuleType("apps.channels.models")
        mock_channels_models.reserve_profile_slot = mock_conn_pool.reserve_profile_slot

        class FakeChannel:
            def __init__(self, account):
                self.id = 1
                self.uuid = "chan-uuid-1"
                self.account = account

            def get_stream(self, requester=None, allowed_m3u_profiles=None):
                m3u_profiles = self.account.profiles.filter(is_active=True)
                default_profile = next(
                    (obj for obj in m3u_profiles if obj.is_default), None
                )
                if not default_profile:
                    return None, None, "No active profiles", False
                profiles = [default_profile] + [
                    obj for obj in m3u_profiles if not obj.is_default
                ]
                for profile in profiles:
                    reserved, _count, _reason = mock_channels_models.reserve_profile_slot(
                        profile, self.account._redis
                    )
                    if reserved:
                        return 999, profile.id, None, True
                return None, None, "All active M3U profiles have reached maximum connection limits", False

        class FakeStream:
            def __init__(self, account):
                self.id = 55
                self.m3u_account = account

            def get_stream(self, preferred_profile_id=None):
                if preferred_profile_id is not None:
                    prof = self.m3u_account.profiles.filter(
                        id=preferred_profile_id, is_active=True
                    ).first()
                    if not prof:
                        return None, None, "Unavailable", False
                    reserved, _, _ = mock_channels_models.reserve_profile_slot(
                        prof, self.m3u_account._redis
                    )
                    return (self.id, prof.id, None, True) if reserved else (None, None, "Full", False)

                m3u_profiles = self.m3u_account.profiles.all()
                default_profile = next(
                    (obj for obj in m3u_profiles if obj.is_default), None
                )
                profiles = [default_profile] + [
                    obj for obj in m3u_profiles if not obj.is_default
                ]
                for profile in profiles:
                    if not profile.is_active:
                        continue
                    reserved, _, _ = mock_channels_models.reserve_profile_slot(
                        profile, self.m3u_account._redis
                    )
                    if reserved:
                        return self.id, profile.id, None, True
                return None, None, "Full", False

        self.account._redis = self.fake_redis
        mock_channels_models.Channel = FakeChannel
        mock_channels_models.Stream = FakeStream
        sys.modules["apps.channels"] = types.ModuleType("apps.channels")
        sys.modules["apps.channels.models"] = mock_channels_models

        # 6. Mock apps.proxy.live_proxy.url_utils
        mock_url_utils = types.ModuleType("apps.proxy.live_proxy.url_utils")

        def pick_stream_m3u_profile_id(stream, allowed_m3u_profiles=None):
            m3u_profiles = list(stream.m3u_account.profiles.filter(is_active=True))
            default_profile = next((p for p in m3u_profiles if p.is_default), None)
            if not default_profile:
                return None
            candidates = [default_profile] + [p for p in m3u_profiles if not p.is_default]
            for profile in candidates:
                if pool_has_capacity_for_profile(profile, self.fake_redis):
                    return profile.id
            return None

        mock_url_utils.pick_stream_m3u_profile_id = pick_stream_m3u_profile_id
        mock_url_utils.get_stream_info_for_switch = lambda *a, **kw: None
        mock_url_utils.get_alternate_streams = lambda *a, **kw: []
        sys.modules["apps.proxy"] = types.ModuleType("apps.proxy")
        sys.modules["apps.proxy.live_proxy"] = types.ModuleType("apps.proxy.live_proxy")
        sys.modules["apps.proxy.live_proxy.url_utils"] = mock_url_utils

        # 7. Mock apps.proxy.vod_proxy.views._get_m3u_profile
        mock_vod_views = types.ModuleType("apps.proxy.vod_proxy.views")

        class FakeM3UAccountProfileManager:
            def __init__(self, account):
                self._account = account

            def filter(self, **kwargs):
                return self._account.profiles.filter(**kwargs)

            def select_related(self, *args):
                return self._account.profiles

        class FakeM3UAccountProfileModel:
            DoesNotExist = Exception

        FakeM3UAccountProfileModel.objects = FakeM3UAccountProfileManager(self.account)
        mock_vod_views.M3UAccountProfile = FakeM3UAccountProfileModel

        def _get_m3u_profile(
            m3u_account, profile_id, session_id=None, restrict_to_profile_ids=None
        ):
            if session_id:
                conn_data = self.fake_redis.hgetall(f"vod_persistent_connection:{session_id}")
                if conn_data and conn_data.get("m3u_profile_id"):
                    pid = int(conn_data["m3u_profile_id"])
                    prof = m3u_account.profiles.filter(id=pid, is_active=True).first()
                    if prof:
                        return (prof, get_profile_connection_count(prof, self.fake_redis))

            if profile_id and (
                restrict_to_profile_ids is None or profile_id in restrict_to_profile_ids
            ):
                prof = m3u_account.profiles.filter(id=profile_id, is_active=True).first()
                if prof and pool_has_capacity_for_profile(prof, self.fake_redis):
                    return (prof, get_profile_connection_count(prof, self.fake_redis))

            m3u_profiles = m3u_account.profiles.filter(is_active=True)
            if restrict_to_profile_ids is not None:
                m3u_profiles = m3u_profiles.filter(id__in=restrict_to_profile_ids)
            default_profile = m3u_profiles.filter(is_default=True).first()
            if not default_profile:
                return None
            profiles = [default_profile] + list(m3u_profiles.filter(is_default=False))
            for prof in profiles:
                if pool_has_capacity_for_profile(prof, self.fake_redis):
                    return (prof, get_profile_connection_count(prof, self.fake_redis))
            return None

        mock_vod_views._get_m3u_profile = _get_m3u_profile
        sys.modules["apps.proxy.vod_proxy"] = types.ModuleType("apps.proxy.vod_proxy")
        sys.modules["apps.proxy.vod_proxy.views"] = mock_vod_views

        # 8. Mock apps.timeshift.views._prepare_catchup_stream_attempt
        mock_ts_views = types.ModuleType("apps.timeshift.views")
        mock_ts_views.reserve_profile_slot = mock_conn_pool.reserve_profile_slot

        def _prepare_catchup_stream_attempt(
            catchup_stream, *, timestamp, redis_client, reserve, profile=None
        ):
            m3u_account = catchup_stream.m3u_account
            m3u_profiles = list(m3u_account.profiles.filter(is_active=True))
            default_profile = next((p for p in m3u_profiles if p.is_default), None)
            if default_profile is None and profile is None:
                return None, False
            timezone_profile = default_profile or profile
            profile_walk = (
                [profile]
                if profile is not None
                else [default_profile] + [p for p in m3u_profiles if not p.is_default]
            )
            for cand in profile_walk:
                if reserve:
                    ok, _, _ = mock_ts_views.reserve_profile_slot(cand, redis_client)
                    if ok:
                        return (
                            SimpleNamespace(
                                profile=cand, timezone_profile=timezone_profile
                            ),
                            False,
                        )
                elif pool_has_capacity_for_profile(cand, redis_client):
                    return (
                        SimpleNamespace(
                            profile=cand, timezone_profile=timezone_profile
                        ),
                        False,
                    )
            return None, True

        mock_ts_views._prepare_catchup_stream_attempt = _prepare_catchup_stream_attempt
        sys.modules["apps.timeshift"] = types.ModuleType("apps.timeshift")
        sys.modules["apps.timeshift.views"] = mock_ts_views

        # 9. Mock apps.m3u.utils.get_allowed_m3u_profiles & apps.m3u.models.M3UAccount
        mock_m3u_utils = types.ModuleType("apps.m3u.utils")
        mock_m3u_utils.get_allowed_m3u_profiles = lambda user: {
            10: [self.prof1, self.prof2, self.prof3]
        }
        sys.modules["apps.m3u.utils"] = mock_m3u_utils

        mock_m3u_models = types.ModuleType("apps.m3u.models")
        mock_m3u_account_cls = MagicMock()
        mock_m3u_account_cls.objects.filter.return_value.prefetch_related.return_value = [
            self.account
        ]
        mock_m3u_models.M3UAccount = mock_m3u_account_cls
        sys.modules["apps.m3u.models"] = mock_m3u_models

        sys.modules.pop("apps.plugins.models", None)

        import plugin

        self.plugin_mod = plugin
        self.plugin = plugin.Plugin()

    def tearDown(self):
        if hasattr(self, "plugin") and self.plugin:
            self.plugin.stop(context={"reason": "tearDown"})
        sys.modules.pop("apps.plugins.models", None)

    def test_live_channel_least_connections_balancing(self):
        """
        Verify new channel connections spread evenly across cred1 -> cred2 -> cred3
        instead of saturating cred1 before touching cred2/cred3.
        """
        Channel = sys.modules["apps.channels.models"].Channel
        chan = Channel(self.account)

        # Round 1: 0 connections on all -> cred1 (101), then cred2 (102), then cred3 (103)
        _, p1, _, _ = chan.get_stream()
        _, p2, _, _ = chan.get_stream()
        _, p3, _, _ = chan.get_stream()
        self.assertEqual([p1, p2, p3], [101, 102, 103])

        # Round 2: all at 1 connection -> cred1 (101), then cred2 (102), then cred3 (103)
        _, p4, _, _ = chan.get_stream()
        _, p5, _, _ = chan.get_stream()
        _, p6, _, _ = chan.get_stream()
        self.assertEqual([p4, p5, p6], [101, 102, 103])

        # Simulate a stream disconnecting from cred2 (102): counts are now 101=2, 102=1, 103=2
        self.fake_redis.decr("profile_connections:102")

        # Next stream MUST pick cred2 (102) because it has 1 < 2 connections
        _, p7, _, _ = chan.get_stream()
        self.assertEqual(p7, 102)

    def test_skips_profile_at_capacity_with_lower_absolute_connections(self):
        """
        If cred1 has max_streams=1 and is at 1/1 (full), while cred2 is at 2/5 and cred3 is at 1/5,
        Trimarr must skip cred1 (at capacity) and pick cred3 (1/5 < 2/5).
        """
        self.prof1.max_streams = 1
        self.prof2.max_streams = 5
        self.prof3.max_streams = 5

        self.fake_redis.set("profile_connections:101", 1)  # 1/1 FULL
        self.fake_redis.set("profile_connections:102", 2)  # 2/5 available
        self.fake_redis.set("profile_connections:103", 1)  # 1/5 available (least used)

        Channel = sys.modules["apps.channels.models"].Channel
        chan = Channel(self.account)

        _, selected_id, _, _ = chan.get_stream()
        self.assertEqual(selected_id, 103)

    def test_server_group_shared_credential_pool_awareness(self):
        """
        If cred2 has 0 local profile connections but 2 connections via a shared ServerGroup
        credential pool, while cred3 has 1 connection, cred3 (1 < 2) must be selected.
        """
        self.fake_redis.set("profile_connections:101", 2)
        self.fake_redis.set("profile_connections:102", 0)
        self.cred_counts[102] = 2  # Shared ServerGroup login has 2 active streams
        self.fake_redis.set("profile_connections:103", 1)

        Channel = sys.modules["apps.channels.models"].Channel
        chan = Channel(self.account)

        _, selected_id, _, _ = chan.get_stream()
        self.assertEqual(selected_id, 103)

    def test_vod_least_connections_and_session_stickiness(self):
        """
        Verify VOD _get_m3u_profile selects the least-used profile for new sessions,
        but keeps existing sessions sticky to their assigned profile for HTTP Range requests.
        """
        vod_views = sys.modules["apps.proxy.vod_proxy.views"]

        self.fake_redis.set("profile_connections:101", 2)
        self.fake_redis.set("profile_connections:102", 1)
        self.fake_redis.set("profile_connections:103", 0)

        # New session without existing Redis hash selects cred3 (103, 0 connections)
        prof, count = vod_views._get_m3u_profile(self.account, None, session_id="new-sess")
        self.assertEqual(prof.id, 103)
        self.assertEqual(count, 0)

        # Existing session bound to cred1 (101) stays on cred1 even though cred3 has 0 connections
        self.fake_redis.hset("vod_persistent_connection:existing-sess", "m3u_profile_id", "101")
        prof_sticky, _ = vod_views._get_m3u_profile(
            self.account, None, session_id="existing-sess"
        )
        self.assertEqual(prof_sticky.id, 101)

    def test_timeshift_least_connections_preserves_default_timezone_profile(self):
        """
        Verify catchup stream selection picks the least-used profile while preserving
        default_profile for provider timezone resolution.
        """
        ts_views = sys.modules["apps.timeshift.views"]

        self.fake_redis.set("profile_connections:101", 2)
        self.fake_redis.set("profile_connections:102", 0)
        self.fake_redis.set("profile_connections:103", 1)

        catchup_stream = SimpleNamespace(m3u_account=self.account)
        target, blocked = ts_views._prepare_catchup_stream_attempt(
            catchup_stream,
            timestamp=1700000000,
            redis_client=self.fake_redis,
            reserve=True,
            profile=None,
        )
        self.assertFalse(blocked)
        self.assertIsNotNone(target)
        self.assertEqual(target.profile.id, 102)
        # Timezone profile must still be the account's true default profile (101)
        self.assertEqual(target.timezone_profile.id, 101)

    def test_allowed_m3u_profiles_sorted_by_least_used(self):
        """Verify get_allowed_m3u_profiles returns each account's allowlist sorted by least load."""
        m3u_utils = sys.modules["apps.m3u.utils"]

        self.fake_redis.set("profile_connections:101", 2)
        self.fake_redis.set("profile_connections:102", 1)
        self.fake_redis.set("profile_connections:103", 0)

        allowed = m3u_utils.get_allowed_m3u_profiles(user=SimpleNamespace())
        self.assertEqual([p.id for p in allowed[10]], [103, 102, 101])

    def test_unlimited_profiles_tracked_and_balanced(self):
        """
        Verify that profiles with max_streams=0 (unlimited) still increment Redis counters
        when track_unlimited_profiles=True so they load-balance evenly.
        """
        self.prof1.max_streams = 0
        self.prof2.max_streams = 0
        self.prof3.max_streams = 0

        Channel = sys.modules["apps.channels.models"].Channel
        chan = Channel(self.account)

        _, p1, _, _ = chan.get_stream()
        _, p2, _, _ = chan.get_stream()
        _, p3, _, _ = chan.get_stream()
        self.assertEqual([p1, p2, p3], [101, 102, 103])
        self.assertEqual(int(self.fake_redis.get("profile_connections:101")), 1)
        self.assertEqual(int(self.fake_redis.get("profile_connections:102")), 1)
        self.assertEqual(int(self.fake_redis.get("profile_connections:103")), 1)

    def test_utilization_tie_breaker(self):
        """
        When tied at 1 active connection, `utilization` tie-breaker prefers the profile
        with larger max_streams (1/5 = 20% utilization over 1/2 = 50% utilization).
        """
        self.plugin._cached_settings["tie_breaker"] = "utilization"
        self.prof1.max_streams = 2
        self.prof2.max_streams = 5
        self.prof3.max_streams = 2

        self.fake_redis.set("profile_connections:101", 1)  # 50%
        self.fake_redis.set("profile_connections:102", 1)  # 20%
        self.fake_redis.set("profile_connections:103", 1)  # 50%

        Channel = sys.modules["apps.channels.models"].Channel
        chan = Channel(self.account)

        _, selected_id, _, _ = chan.get_stream()
        self.assertEqual(selected_id, 102)

    def test_disabling_plugin_or_stopping_restores_linear_failover(self):
        """
        Verify toggling enabled=False or calling stop() immediately reverts Dispatcharr
        to its default linear failover (cred1 until full 3/3, then cred2).
        """
        self.plugin._cached_settings["enabled"] = False

        Channel = sys.modules["apps.channels.models"].Channel
        chan = Channel(self.account)

        # With Trimarr disabled, cred1 (max_streams=3) takes all first 3 connections
        _, p1, _, _ = chan.get_stream()
        _, p2, _, _ = chan.get_stream()
        _, p3, _, _ = chan.get_stream()
        _, p4, _, _ = chan.get_stream()
        self.assertEqual([p1, p2, p3, p4], [101, 101, 101, 102])

    def test_ui_actions_status_and_inspect(self):
        """Verify status_check and inspect_profiles UI actions return accurate diagnostics."""
        self.fake_redis.set("profile_connections:101", 2)
        self.fake_redis.set("profile_connections:102", 0)
        self.fake_redis.set("profile_connections:103", 1)

        status_res = self.plugin.run("status_check", {}, {})
        self.assertEqual(status_res["status"], "ok")
        self.assertTrue(status_res["is_enabled"])
        self.assertTrue(status_res["is_hooked"])

        inspect_res = self.plugin.run("inspect_profiles", {}, {})
        self.assertEqual(inspect_res["status"], "ok")
        self.assertEqual(len(inspect_res["accounts"]), 1)
        self.assertEqual(inspect_res["accounts"][0]["next_profile_id"], 102)
        self.assertIn("cred2 (id=102)", inspect_res["message"])

    def test_random_tie_breaker_queryset_consistency(self):
        """
        Verify that with tie_breaker='random', iterating the same QuerySet twice
        (once for `default_profile` and once for `[not is_default]`) always yields
        all 3 distinct profiles with no duplicates or dropped profiles.
        """
        self.plugin._cached_settings["tie_breaker"] = "random"

        for _ in range(30):
            with self.plugin_mod._profile_selection_context():
                m3u_profiles = self.account.profiles.filter(is_active=True)
                default_profile = next(
                    (obj for obj in m3u_profiles if obj.is_default), None
                )
                self.assertIsNotNone(default_profile)
                profiles = [default_profile] + [
                    obj for obj in m3u_profiles if not obj.is_default
                ]
                self.assertEqual(len(profiles), 3)
                self.assertEqual({p.id for p in profiles}, {101, 102, 103})

    def test_live_db_settings_update_not_stale_cached(self):
        """
        Verify that updating PluginConfig.settings in Dispatcharr's database
        is immediately reflected on the very next stream selection without restart.
        """
        mock_config = MagicMock()
        mock_config.enabled = True
        mock_config.settings = {"enabled": True, "tie_breaker": "default_first"}

        mock_models = types.ModuleType("apps.plugins.models")
        mock_plugin_config_cls = MagicMock()
        mock_plugin_config_cls.objects.filter.return_value.first.return_value = mock_config
        mock_models.PluginConfig = mock_plugin_config_cls
        sys.modules["apps.plugins.models"] = mock_models

        Channel = sys.modules["apps.channels.models"].Channel
        chan = Channel(self.account)

        # Enabled: first 2 streams pick 101 then 102
        _, p1, _, _ = chan.get_stream()
        _, p2, _, _ = chan.get_stream()
        self.assertEqual([p1, p2], [101, 102])

        # Disable via DB settings: next stream reverts to linear failover (101 has 1/3 capacity)
        mock_config.settings = {"enabled": False, "tie_breaker": "default_first"}
        _, p3, _, _ = chan.get_stream()
        self.assertEqual(p3, 101)


if __name__ == "__main__":
    unittest.main()
