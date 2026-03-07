
# Fires NASA FIRMS Unofficial Integration for Home Assistant
##  Fire Information for Resource Management System

<img src="https://raw.githubusercontent.com/janfajessen/Fires-NASA-FIRMS-Unofficial/refs/heads/main/logo.png" width="150"/>

[![HACS Custom](https://img.shields.io/badge/HACS-Custom-41bdf5.svg?style=for-the-badge)](https://hacs.xyz/docs/publish/start)
[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-Donate-yellow.svg?style=for-the-badge)](https://www.buymeacoffee.com/janfajessen)  <!-- Reemplaza con tu link -->
[![Patreon](https://img.shields.io/badge/Patreon-Support-red.svg?style=for-the-badge)](https://www.patreon.com/janfajessen)  <!-- Reemplaza con tu link -->

This is an unofficial custom integration for Home Assistant that fetches active fire data from NASA's FIRMS (Fire Information for Resource Management System) API. It displays detected fires as geo_location entities on your HA map, with distance from home as state, and detailed attributes like confidence level, acquisition time, and more.

[<img src="https://raw.githubusercontent.com/janfajessen/Fires-NASA-FIRMS-Unofficial/refs/heads/main/Map_NASA_FIRMS.png" width="200"/>](https://firms.modaps.eosdis.nasa.gov/map/#d:24hrs;@20.1,31.3,4.5z)

## Features
- Fetches real-time fire data from NASA FIRMS within a configurable radius.
- Supports multiple data sources (VIIRS SNPP, MODIS, VIIRS NOAA-20).
- Filters by confidence level (low, nominal, high).
- Displays each fire as a geo_location entity with distance (km/mi) as state.
- Customizable scan interval, days back, and units.
- Multi-language support (32 languages included).
- Attributes include latitude, longitude, brightness, FRP, local acquisition time/date, and more.
- Multiple instances supported for different locations/configs.

## Requirements
- Home Assistant 2026.2 or later.
- API key from [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/api/) (free, quick signup).
- Dependencies: `aiohttp>=3.8.0`, `haversine>=2.8.0` (installed automatically via HA).

## Installation

### Via HACS (Recommended)
1. Open HACS in Home Assistant: **HACS > Integrations**.
2. Click the three dots (⋮) > **Custom repositories**.
3. Add repository: URL = `https://github.com/janfajessen/Fires-NASA-FIRMS-Unofficial`, Category = "Integration".
4. Search for "NASA FIRMS Fires" in HACS and install.
5. Restart Home Assistant.
6. Add the integration: **Settings > Devices & Services > Add Integration > NASA FIRMS Fires**.

### Manual Installation
1. Download the latest release ZIP from [Releases](https://github.com/janfajessen/Fires-NASA-FIRMS-Unofficial/releases).
2. Extract to `/config/custom_components/firms_nasa_fires/`.
3. Restart Home Assistant.
4. Add the integration as above.

## Configuration
<img src="https://raw.githubusercontent.com/janfajessen/Fires-NASA-FIRMS-Unofficial/refs/heads/main/config_flow.png" width="100"/>
Go to **Settings > Devices & Services > Add Integration > NASA FIRMS Fires**.

- **API Key**: Your NASA FIRMS key.
- **Latitude/Longitude**: Custom location (defaults to HA home).
- **Radius (km)**: Search area (10-500 km).
- **Units**: km or mi.
- **Min Confidence**: Low, Nominal, High.
- **Source**: VIIRS SNPP, MODIS, VIIRS NOAA-20.
- **Days Back**: 1-5 days.
- **Update Interval**: 5-120 minutes.

Options can be changed later via the integration's Options menu.

## Usage
- Fires appear as geo_location entities (e.g., `geo_location.high_conf_fire_nasa_firms_39_46_0_37`).
- View on HA Map: Distance from coordinates as state.
- Attributes: Confidence, brightness, FRP, local time/date, etc.
- Automations suggest: Trigger on new fires or distance changes.

## Support & Donations
If you like this integration, consider supporting development!

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-Donate-yellow.svg?style=for-the-badge)](https://www.buymeacoffee.com/janfajessen)
[![Patreon](https://img.shields.io/badge/Patreon-Support-red.svg?style=for-the-badge)](https://www.patreon.com/janfajessen)

Issues? Open one on [GitHub](https://github.com/janfajessen/Fires-NASA-FIRMS-Unofficial/issues).

This integration should only be used for your own educational purposes.
## License
MIT License. See [LICENSE](LICENSE) for details.

---

Thanks for using NASA FIRMS Fires! If you have feedback, star the repo or contribute.
