"""Flujo de configuración para NASA FIRMS Fires."""
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.helpers import selector

from .const import (
    DOMAIN,
    MAX_INSTANCES,
    CONF_API_KEY,
    CONF_RADIUS_KM,
    CONF_UNITS,
    CONF_MIN_CONFIDENCE,
    CONF_DAYS,
    CONF_SOURCE,
    CONF_SCAN_INTERVAL,
    CONF_LATITUDE,
    CONF_LONGITUDE,
    DEFAULT_RADIUS_KM,
    DEFAULT_UNITS,
    DEFAULT_MIN_CONFIDENCE,
    DEFAULT_DAYS,
    DEFAULT_SOURCE,
    DEFAULT_SCAN_INTERVAL,
    MIN_DAYS,
    MAX_DAYS,
    MIN_RADIUS,
    MAX_RADIUS,
    MIN_SCAN_INTERVAL,
    MAX_SCAN_INTERVAL,
)

# Opciones de fuente para los selectores (label = clave de traducción)
_SOURCE_OPTIONS = [
    {"value": "VIIRS_SNPP_NRT",   "label": "viirs_snpp"},
    {"value": "MODIS_NRT",        "label": "modis"},
    {"value": "VIIRS_NOAA20_NRT", "label": "viirs_noaa20"},
    {"value": "VIIRS_NOAA21_NRT", "label": "viirs_noaa21"},
]

def _source_selector():
    return selector.SelectSelector(
        selector.SelectSelectorConfig(
            options=_SOURCE_OPTIONS,
            multiple=True,          # Multi-select: el usuario elige 1 o más fuentes
            mode=selector.SelectSelectorMode.LIST,
            translation_key="source",
        )
    )

def _unit_selector():
    return selector.SelectSelector(
        selector.SelectSelectorConfig(
            options=[
                {"value": "km", "label": "km"},
                {"value": "mi", "label": "mi"},
            ],
            mode=selector.SelectSelectorMode.DROPDOWN,
            translation_key="units",
        )
    )

def _confidence_selector():
    return selector.SelectSelector(
        selector.SelectSelectorConfig(
            options=[
                {"value": "l", "label": "l"},
                {"value": "n", "label": "n"},
                {"value": "h", "label": "h"},
            ],
            mode=selector.SelectSelectorMode.DROPDOWN,
            translation_key="confidence",
        )
    )


class FirmsConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        errors = {}

        existing = self.hass.config_entries.async_entries(DOMAIN)
        if len(existing) >= MAX_INSTANCES:
            return self.async_abort(reason="max_instances_reached")

        if user_input is not None:
            api_key = user_input.get(CONF_API_KEY, "")

            if len(api_key) < 10:
                errors[CONF_API_KEY] = "api_key_invalid"
            elif not user_input.get(CONF_SOURCE):
                errors[CONF_SOURCE] = "source_required"
            else:
                lat = user_input[CONF_LATITUDE]
                lon = user_input[CONF_LONGITUDE]
                await self.async_set_unique_id(f"{DOMAIN}_{lat}_{lon}")
                self._abort_if_unique_id_configured()

                instance_num = len(existing) + 1
                title = (
                    f"NASA FIRMS Fires #{instance_num}"
                    if instance_num > 1
                    else "NASA FIRMS Fires"
                )

                return self.async_create_entry(
                    title=title,
                    data={
                        CONF_API_KEY:  user_input[CONF_API_KEY],
                        CONF_LATITUDE: lat,
                        CONF_LONGITUDE: lon,
                    },
                    options={
                        CONF_RADIUS_KM:      user_input[CONF_RADIUS_KM],
                        CONF_UNITS:          user_input.get(CONF_UNITS, DEFAULT_UNITS),
                        CONF_MIN_CONFIDENCE: user_input.get(CONF_MIN_CONFIDENCE, DEFAULT_MIN_CONFIDENCE),
                        CONF_SOURCE:         user_input.get(CONF_SOURCE, DEFAULT_SOURCE),
                        CONF_DAYS:           user_input.get(CONF_DAYS, DEFAULT_DAYS),
                        CONF_SCAN_INTERVAL:  user_input.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL),
                    },
                )

        data_schema = vol.Schema({
            vol.Required(CONF_API_KEY): selector.TextSelector(
                selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
            ),
            vol.Required(CONF_LATITUDE, default=self.hass.config.latitude): selector.NumberSelector(
                selector.NumberSelectorConfig(min=-90, max=90, step=0.001, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(CONF_LONGITUDE, default=self.hass.config.longitude): selector.NumberSelector(
                selector.NumberSelectorConfig(min=-180, max=180, step=0.001, mode=selector.NumberSelectorMode.BOX)
            ),
            vol.Required(CONF_RADIUS_KM, default=DEFAULT_RADIUS_KM): selector.NumberSelector(
                selector.NumberSelectorConfig(min=MIN_RADIUS, max=MAX_RADIUS, unit_of_measurement="km", mode=selector.NumberSelectorMode.SLIDER)
            ),
            vol.Optional(CONF_UNITS, default=DEFAULT_UNITS): _unit_selector(),
            vol.Optional(CONF_MIN_CONFIDENCE, default=DEFAULT_MIN_CONFIDENCE): _confidence_selector(),
            vol.Optional(CONF_SOURCE, default=DEFAULT_SOURCE): _source_selector(),
            vol.Optional(CONF_DAYS, default=DEFAULT_DAYS): selector.NumberSelector(
                selector.NumberSelectorConfig(min=MIN_DAYS, max=MAX_DAYS, unit_of_measurement="days", mode=selector.NumberSelectorMode.SLIDER)
            ),
            vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL): selector.NumberSelector(
                selector.NumberSelectorConfig(min=MIN_SCAN_INTERVAL, max=MAX_SCAN_INTERVAL, unit_of_measurement="min", mode=selector.NumberSelectorMode.SLIDER)
            ),
        })

        return self.async_show_form(step_id="user", data_schema=data_schema, errors=errors)

    @staticmethod
    def async_get_options_flow(config_entry):
        return FirmsOptionsFlow()


class FirmsOptionsFlow(config_entries.OptionsFlow):
    """Sin __init__ personalizado: self.config_entry lo provee la base class (HA 2024.11+)."""

    async def async_step_init(self, user_input=None):
        errors = {}

        if user_input is not None:
            if not user_input.get(CONF_SOURCE):
                errors[CONF_SOURCE] = "source_required"
            else:
                return self.async_create_entry(data=user_input)

        # Compatibilidad: si la opción guardada era string (instalación antigua), convertir a lista
        current_source = self.config_entry.options.get(CONF_SOURCE, DEFAULT_SOURCE)
        if isinstance(current_source, str):
            current_source = [current_source]

        data_schema = vol.Schema({
            vol.Required(CONF_RADIUS_KM, default=self.config_entry.options.get(CONF_RADIUS_KM, DEFAULT_RADIUS_KM)): selector.NumberSelector(
                selector.NumberSelectorConfig(min=MIN_RADIUS, max=MAX_RADIUS, unit_of_measurement="km", mode=selector.NumberSelectorMode.SLIDER)
            ),
            vol.Required(CONF_UNITS, default=self.config_entry.options.get(CONF_UNITS, DEFAULT_UNITS)): _unit_selector(),
            vol.Required(CONF_MIN_CONFIDENCE, default=self.config_entry.options.get(CONF_MIN_CONFIDENCE, DEFAULT_MIN_CONFIDENCE)): _confidence_selector(),
            vol.Required(CONF_SOURCE, default=current_source): _source_selector(),
            vol.Required(CONF_DAYS, default=self.config_entry.options.get(CONF_DAYS, DEFAULT_DAYS)): selector.NumberSelector(
                selector.NumberSelectorConfig(min=MIN_DAYS, max=MAX_DAYS, unit_of_measurement="days", mode=selector.NumberSelectorMode.SLIDER)
            ),
            vol.Required(CONF_SCAN_INTERVAL, default=self.config_entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)): selector.NumberSelector(
                selector.NumberSelectorConfig(min=MIN_SCAN_INTERVAL, max=MAX_SCAN_INTERVAL, unit_of_measurement="min", mode=selector.NumberSelectorMode.SLIDER)
            ),
        })

        return self.async_show_form(step_id="init", data_schema=data_schema, errors=errors)
        
