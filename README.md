<div align="center">
  
# Fires NASA FIRMS Integration

## custom component <br> for Home Assistant

Fire Information for Resource Management System

<a href="https://firms.modaps.eosdis.nasa.gov">
  <img src="brands/logo@2x.png" width="450"/>
</a>

![Version](https://img.shields.io/badge/version-1.5.24-blue?style=for-the-badge)
![HA](https://img.shields.io/badge/Home%20Assistant-2024.1+-orange?style=for-the-badge&logo=home-assistant)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.12+-3776AB?style=for-the-badge&logo=python)
![HACS](https://img.shields.io/badge/HACS-Custom-41bdf5?style=for-the-badge)
[![Ko-Fi](https://img.shields.io/badge/Ko--Fi-Support-teal?style=for-the-badge&logo=ko-fi)](https://ko-fi.com/janfajessen)
[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-Donate-yellow?style=for-the-badge&logo=buymeacoffee)](https://www.buymeacoffee.com/janfajessen)
[![Patreon](https://img.shields.io/badge/Patreon-Support-red?style=for-the-badge&logo=patreon)](https://www.patreon.com/janfajessen)
[![GitHub Sponsors](https://img.shields.io/badge/GitHub%20Sponsors-Support-pink?style=for-the-badge&logo=githubsponsors)](https://github.com/sponsors/janfajessen)
<!--[![PayPal](https://img.shields.io/badge/PayPal-Donate-blue?style=for-the-badge&logo=paypal)](https://paypal.me/janfajessen)-->

</div>

This is a custom integration for Home Assistant that fetches active fire data from [NASA FIRMS free API](https://firms.modaps.eosdis.nasa.gov/api/)  <br> (Fire Information for Resource Management System).  
It displays detected fires as `geo_location` entities on your HA map, with distance coordinates as state, and detailed attributes like confidence level, acquisition time, and more.

---

## Features

- Fetches real-time fire data from NASA FIRMS within a configurable radius.
- Filters by confidence level (low, nominal, high).
- Displays each fire as a `geo_location` entity with distance (km/mi) as state from custom latitude and longitude.
- **Customizable scan interval** (5–120 minutes) — fires update automatically on schedule.
- **Multiple satellite sources** — choose one or more: VIIRS SNPP, MODIS, VIIRS NOAA-20, VIIRS NOAA-21.
- **Automatic cross-satellite deduplication** — same fire detected by multiple satellites appears only once (best confidence/FRP kept).
- Configurable days back (1–5) and search radius (10–500 km).
- **Multiple instances** supported — up to 10 simultaneous instances for different locations or configurations.
- Multi-language support.
- Attributes include latitude, longitude, brightness, FRP, local acquisition time/date, satellite source, and more.
- **Options can be changed** at any time via the integration's ⚙️ Options menu without data loss.

---

## Satellite Sources

All four sources detect active fire/thermal anomalies. The difference is timing and resolution:

| Source | Resolution | Daily passes | Notes |
|---|---|---|---|
| VIIRS SNPP | 375 m | ~2 | Active since 2012 |
| MODIS | 1 km | ~4 | Older, lower resolution, more passes |
| VIIRS NOAA-20 | 375 m | ~2 | Active since 2018 |
| VIIRS NOAA-21 | 375 m | ~2 | Active since Jan 2024, newest |

Using multiple sources together gives better temporal coverage (fires detected at different times of day). Duplicates are removed automatically.

---

## Requirements

- Home Assistant **2026.2** or later.
- API key from [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/api/) (free, quick signup).
- Dependencies: `aiohttp>=3.8.0`, `haversine>=2.8.0` (installed automatically via HA).

---

## Installation

### Via HACS (Recommended)

1. Open HACS in Home Assistant: **HACS > Integrations**.
2. Click the three dots (⋮) > **Custom repositories**.
3. Add repository URL: `https://github.com/janfajessen/Fires-NASA-FIRMS-Unofficial`, Category = `Integration`.
4. Search for **"NASA FIRMS Fires"** in HACS and install.
5. Restart Home Assistant.
6. Add the integration: **Settings > Devices & Services > Add Integration > NASA FIRMS Fires (Unofficial)**.

### Manual Installation

1. Download or clone this repository.
2. Copy the `firms_nasa_fires/` folder to `/config/custom_components/firms_nasa_fires/`.
3. Restart Home Assistant.
4. Add the integration: **Settings > Devices & Services > Add Integration > NASA FIRMS Fires (Unofficial)**.

<a>
  <img src="brands/icon.png" width="100"/>
</a>
---

## Configuration

<a>
<img src="https://raw.githubusercontent.com/janfajessen/Fires-NASA-FIRMS-Unofficial/refs/heads/main/config_flow.png" width="300"/>
</a>

Go to **Settings > Devices & Services > Add Integration > NASA FIRMS Fires (Unofficial)**.

| Parameter | Description |
|---|---|
| **API Key** | Your NASA FIRMS key (get it free at [firms.modaps.eosdis.nasa.gov/api](https://firms.modaps.eosdis.nasa.gov/api/)) |
| **Latitude / Longitude** | Center of the search area (defaults to HA home) |
| **Search radius** | 10–500 km (6.2–310.7 mi). Always defined in km; miles only affect how distances are displayed. |
| **Distance unit** | km or mi — controls the state value of each fire entity |
| **Min confidence** | Low (all), Nominal+High, or High only |
| **Data sources** | One or more satellite sources (multi-select) |
| **Days back** | 1–5 days of historical data to include |
| **Update interval** | 5–120 minutes between automatic data refreshes |

**All options can be modified later** via the ⚙️ button on the integration card.

---

## Multiple Instances

You can add up to **10 simultaneous instances**, each with its own location, radius, sources, and update interval. Useful for monitoring multiple regions or comparing satellite sources independently.

Each instance runs its own independent update schedule — they do not interfere with each other.

---

## Usage

- Fires appear as `geo_location` entities (e.g., `geo_location.high_conf_fire_nasa_firms_42_81_1_54`).
- View on HA Map: fires are shown as pins at their detected location.
- Entity **state** = distance from the configured coordinates (in km or mi).
- Entities are automatically removed when a fire is no longer detected in the latest data.

### Entity Attributes

| Attribute | Description |
|---|---|
| `latitude` / `longitude` | Fire location |
| `brightness` | Primary brightness (source-independent) |
| `brightness_ti4` / `brightness_ti5` | VIIRS-specific brightness channels |
| `brightness_t31` | MODIS-specific secondary brightness |
| `frp` | Fire Radiative Power (MW) — energy intensity |
| `confidence` / `confidence_level` / `confidence_name` | Raw value, normalized level (l/n/h), human name |
| `acquisition_date` / `acquisition_time` | UTC detection time |
| `acquisition_local_date` / `acquisition_local_time` | Detection time in HA timezone |
| `satellite` / `instrument` | Which satellite/sensor detected the fire |
| `source` | API source identifier (e.g., `VIIRS_SNPP_NRT`) |
| `daynight` | D (day) or N (night) pass |
| `distance_km` | Distance in km regardless of display unit |
| `scan` / `track` | Pixel size at detection angle |
| `attribution` | Data credit (NASA FIRMS) |

### Automation Examples

```yaml
automation:
  - alias: "Alert: High confidence fire within 50 km"
    trigger:
      - platform: state
        entity_id: geo_location.high_conf_fire_nasa_firms_*
    condition:
      - condition: template
        value_template: "{{ trigger.to_state.state | float < 50 }}"
    action:
      - service: notify.mobile_app
        data:
          title: "🔥 Fire detected!"
          message: >
            {{ trigger.to_state.name }} detected
            {{ trigger.to_state.state }} km away.
```

### — Fire detected within your FIRMS radius

```yaml
- alias: "FIRMS — Fire detected nearby"
  triggers:
    - trigger: geo_location
      source: firms_nasa_fires
      zone: zone.home
      event: enter
  actions:
    - action: notify.telegram_jan
      data:
        title: "🔥 Fire Detected Nearby"
        message: >
          NASA FIRMS detected a fire near your location.
          Distance: {{ trigger.to_state.state }} {{ trigger.to_state.attributes.unit_of_measurement }}
          {% if state_attr(trigger.entity_id, 'frp') %}
          Fire Radiative Power: {{ state_attr(trigger.entity_id, 'frp') }} MW
          {% endif %}
          {% if state_attr(trigger.entity_id, 'brightness') %}
          Brightness: {{ state_attr(trigger.entity_id, 'brightness') }} K
          {% endif %}
```

### — High intensity fire (FRP > 100 MW)

```yaml
- alias: "FIRMS — High intensity fire nearby"
  triggers:
    - trigger: geo_location
      source: firms_nasa_fires
      zone: zone.home
      event: enter
  conditions:
    - condition: template
      value_template: "{{ state_attr(trigger.entity_id, 'frp') | float(0) > 100 }}"
  actions:
    - action: notify.telegram_jan
      data:
        title: "🔥🔥 HIGH INTENSITY FIRE NEARBY"
        message: >
          NASA FIRMS: HIGH INTENSITY fire detected!
          Distance: {{ trigger.to_state.state }} {{ trigger.to_state.attributes.unit_of_measurement }}
          FRP: {{ state_attr(trigger.entity_id, 'frp') }} MW
          Satellite: {{ state_attr(trigger.entity_id, 'satellite') | default('unknown') }}
          ⚠️ Check local emergency services immediately.
```

---
<a>
  <img src="brands/icon.png" width="100"/>
</a>

## Support & Donations

If you like this integration, consider supporting development!

[![Ko-Fi](https://img.shields.io/badge/Ko--Fi-Support-teal?style=for-the-badge&logo=ko-fi)](https://ko-fi.com/janfajessen)
[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-Donate-yellow?style=for-the-badge&logo=buymeacoffee)](https://www.buymeacoffee.com/janfajessen)
[![Patreon](https://img.shields.io/badge/Patreon-Support-red?style=for-the-badge&logo=patreon)](https://www.patreon.com/janfajessen)
[![GitHub Sponsors](https://img.shields.io/badge/GitHub%20Sponsors-Support-pink?style=for-the-badge&logo=githubsponsors)](https://github.com/sponsors/janfajessen)
<!--[![PayPal](https://img.shields.io/badge/PayPal-Donate-blue?style=for-the-badge&logo=paypal)](https://paypal.me/janfajessen)-->

Issues? Open one on [GitHub](https://github.com/janfajessen/Fires-NASA-FIRMS-Unofficial/issues).

> This integration is for personal and educational use only. Data is provided by NASA FIRMS.

<a href="https://firms.modaps.eosdis.nasa.gov/map/#d:24hrs;@20.1,31.3,4.5z">
  <img src="https://raw.githubusercontent.com/janfajessen/Fires-NASA-FIRMS-Unofficial/refs/heads/main/Map_NASA_FIRMS.png" width="200"/>
</a>

*NASA FIRMS web page*

---

## Changelog

### v1.3.9
- **Fix:** Automatic scan interval now works correctly (`config_entry` parameter added to `DataUpdateCoordinator`).
- **Fix:** Entities now recreate correctly after reload/options change (session-local tracking instead of persistent entity registry).
- **Fix:** Options menu (⚙️) no longer throws Error 500 (OptionsFlow updated for HA 2024.11+ API).
- **Fix:** MODIS brightness fields now read correctly (`brightness`/`bright_t31` instead of VIIRS-only `bright_ti4`/`bright_ti5`).
- **Fix:** `async_remove()` updated — removed deprecated `force_remove=True` parameter.
- **Fix:** Replaced deprecated `async_timeout` with `asyncio.timeout` (Python 3.11+).
- **Fix:** Selector translations now display correctly (label keys aligned with JSON translation files).
- **Fix:** Slider units no longer show literal strings (`days_unit`, `minutes_unit` → `days`, `min`).
- **Fix:** Memory leak — Options change listener no longer duplicates on each reload.
- **New:** Added **VIIRS NOAA-21** as a fourth satellite source.
- **New:** **Multi-select** for satellite sources — choose one or more simultaneously.
- **New:** **Automatic deduplication** — same fire from multiple satellites appears only once (best confidence/FRP kept).
- **New:** Parallel API fetching — multiple sources fetched simultaneously for faster updates.
- **New:** Up to **10 simultaneous instances** supported, with duplicate location detection.
- **New:** `source` attribute added to each fire entity.
- **New:** `brightness` attribute added as source-independent alias.
- **New:** Radius label shows km/mi equivalent for reference.
- **New:** `source_required` validation error when no source is selected.



---

## License

MIT License. See [LICENSE](LICENSE) for details.

---

Thanks for using NASA FIRMS Fires! If you have feedback, star the repo ⭐ or contribute.
