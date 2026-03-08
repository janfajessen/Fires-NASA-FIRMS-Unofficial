"""Inicialización de NASA FIRMS Fires."""
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN, PLATFORMS


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Configurar una entrada de configuración."""
    hass.data.setdefault(DOMAIN, {})
    # Bug #5 fix: guardamos un dict para poder almacenar el coordinator después
    hass.data[DOMAIN][entry.entry_id] = {}

    # Bug #9 fix: el listener se registra UNA sola vez y async_on_unload lo
    # cancela automáticamente al descargar la entry.
    # Antes: async_reload_entry llamaba async_setup_entry, que registraba el
    # listener otra vez → listeners duplicados en cada cambio de opciones.
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Recargar la entry cuando el usuario cambia las opciones."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Descargar una entrada de configuración."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok
    