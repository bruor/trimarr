# Trimarr — Least-Connections Profile Load Balancer for Dispatcharr

**Trimarr** is a native [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr) plugin that changes how provider profile credentials (`M3UAccountProfile`) are selected when opening new upstream connections.

## The Problem: Default Linear Failover

Out of the box, Dispatcharr orders an M3U account's active profiles with the **Default** profile first, followed by secondary profiles (`cred2`, `cred3`, etc.) in database order:

```python
profiles = [default_profile] + [obj for obj in m3u_profiles if not obj.is_default]
```

When streams are started, Dispatcharr always consumes connections from the primary `default_profile` until it reaches `max_streams`, and only then fails over to `cred2`, and then `cred3`:

| Connection # | Default Linear Failover (`max_streams=3`) | **With Trimarr (Least-Connections)** |
| :--- | :--- | :--- |
| **Stream 1** | `cred1` (`1/3`), `cred2` (`0/3`), `cred3` (`0/3`) | `cred1` (`1/3`), `cred2` (`0/3`), `cred3` (`0/3`) |
| **Stream 2** | `cred1` (`2/3`), `cred2` (`0/3`), `cred3` (`0/3`) | `cred1` (`1/3`), **`cred2` (`1/3`)**, `cred3` (`0/3`) |
| **Stream 3** | `cred1` (`3/3`), `cred2` (`0/3`), `cred3` (`0/3`) | `cred1` (`1/3`), `cred2` (`1/3`), **`cred3` (`1/3`)** |
| **Stream 4** | `cred1` (`3/3`), **`cred2` (`1/3`)**, `cred3` (`0/3`) | **`cred1` (`2/3`)**, `cred2` (`1/3`), `cred3` (`1/3`) |

## Features

- **Least-Connections Load Balancing**: Always selects the active profile with available capacity that currently has the **fewest active connections** in Redis (`profile_connections:{id}`).
- **ServerGroup Shared Pool Awareness**: Accounts for shared credential counters (`server_group_connections:{group_id}:{fingerprint}`) when profiles belong to a `ServerGroup`, using `max(profile_connections, credential_connections)` as the effective load.
- **Full Dispatcharr Coverage**:
  - **Live TV Channels** (`Channel.get_stream` — Proxy, FFmpeg/Transcode, and Redirect modes)
  - **Direct Stream & Channel Previews** (`Stream.get_stream`, `pick_stream_m3u_profile_id`)
  - **Live Stream Switching & Failover** (`get_stream_info_for_switch`, `get_alternate_streams`)
  - **VOD Movies & Series Episodes** (`_get_m3u_profile`, while preserving existing VOD session stickiness for mid-playback HTTP `Range` requests)
  - **Timeshift / Catch-up TV** (`_prepare_catchup_stream_attempt`, while preserving the default profile's `server_info` timezone)
  - **User Profile Allowlists** (`get_allowed_m3u_profiles`)
- **Configurable Tie-Breaker**:
  - **Default Profile First (`default_first`)**: Deterministic tie-breaking (default profile first, then ascending profile ID).
  - **Lowest Capacity Utilization Ratio (`utilization`)**: Prefers the profile with the lowest `active / max_streams` percentage when tied on raw connection count.
  - **Random (`random`)**: Randomly selects among tied least-used profiles so even single-stream bursts distribute wear across all credentials over time.
- **Zero-Restart Live Configuration**: Reads settings directly from Dispatcharr's `PluginConfig` database model so changes in the Web UI apply immediately across all uWSGI worker processes.

---

## Installation

### Option 1: Upload Release Archive in Dispatcharr UI
1. Download `trimarr.zip` from the latest GitHub Release (or build it locally: `mkdir -p build/trimarr && cp plugin.json plugin.py README.md build/trimarr/ && (cd build && zip -r ../trimarr.zip trimarr/)`).
2. In Dispatcharr, navigate to **Settings → Plugins → Import Plugin** and upload `trimarr.zip`.
3. Enable **Trimarr**.

### Option 2: Volume Mount
Copy `plugin.json` and `plugin.py` into `/data/plugins/trimarr/` inside your Dispatcharr container and restart Dispatcharr or reload plugins.

---

## Configuration & Actions

### Settings
| Setting | Default | Description |
| :--- | :--- | :--- |
| **Enable Trimarr Load Balancer** (`enabled`) | `true` | Master toggle to enable least-connections profile selection. When disabled, Dispatcharr immediately reverts to standard linear failover. |
| **Tie-Breaker Strategy** (`tie_breaker`) | `default_first` | How ties are broken when multiple profiles share the lowest active connection count (`default_first`, `utilization`, or `random`). |

### UI Actions
- **Check Status (`status_check`)**: Verifies whether Trimarr is enabled and confirms that all runtime hooks (`queryset_iter`, `channels`, `live_proxy`, `vod_proxy`, `timeshift`, `m3u_utils`) are active.
- **Inspect Load (`inspect_profiles`)**: Queries all active M3U accounts and displays live Redis connection counts (`active / max_streams`) for every profile along with the profile that will be chosen for the next connection.

---

## Running Tests

```bash
python3 -m unittest discover -s tests -v
```
