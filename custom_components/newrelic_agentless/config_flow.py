import hashlib
import logging
from typing import Any, Optional

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import CONF_ENV, CONF_LICENSE_KEY, CONF_REGION, DEFAULT_ENV, DEFAULT_REGION, DOMAIN
from .newrelic_client import REGIONS, CannotConnect, InvalidAuth, NewRelicClient

_LOGGER = logging.getLogger(__name__)

# Description of the config flow:
# async_step_user is called when user starts to configure the integration
# eventually we call async_create_entry with a dictionnary of data
# HA calls async_setup_entry with a ConfigEntry which wraps this data (defined in __init__.py)


class SetupConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: Optional[dict[str, Any]] = None):
        errors: dict[str, str] = {}
        if user_input is not None:
            user_input[CONF_LICENSE_KEY] = user_input[CONF_LICENSE_KEY].strip()
            client = NewRelicClient(
                async_get_clientsession(self.hass),
                user_input[CONF_LICENSE_KEY],
                user_input[CONF_REGION],
            )
            try:
                await client.validate()
            except InvalidAuth:
                errors["base"] = "invalid_auth"
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Unexpected error while validating the New Relic license key")
                errors["base"] = "unknown"

            if not errors:
                # Don't use the raw key as unique id; one entry per key + env.
                unique = hashlib.sha256(
                    f"{user_input[CONF_LICENSE_KEY]}|{user_input[CONF_ENV]}".encode()
                ).hexdigest()
                await self.async_set_unique_id(unique)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"New Relic ({user_input[CONF_REGION]}, {user_input[CONF_ENV]})",
                    data=user_input,
                )

        defaults = user_input or {}
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_LICENSE_KEY, default=defaults.get(CONF_LICENSE_KEY, "")): str,
                vol.Required(CONF_REGION, default=defaults.get(CONF_REGION, DEFAULT_REGION)): vol.In(list(REGIONS)),
                vol.Required(CONF_ENV, default=defaults.get(CONF_ENV, DEFAULT_ENV)): str,
            }),
            errors=errors,
        )
