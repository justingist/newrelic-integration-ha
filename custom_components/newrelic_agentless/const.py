DOMAIN = "newrelic_agentless"

CONF_LICENSE_KEY = "license_key"
CONF_REGION = "region"
CONF_ENV = "env"

DEFAULT_REGION = "US"
DEFAULT_ENV = "prod"

# Custom event type under which non-metric Home Assistant events are stored.
# Query with: SELECT * FROM HomeAssistantEvent SINCE 1 hour ago
EVENT_TYPE = "HomeAssistantEvent"
