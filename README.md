# New Relic Agentless integration for Home Assistant

This integration sends metrics and events from Home Assistant straight to New Relic's
[Metric API](https://docs.newrelic.com/docs/data-apis/ingest-apis/metric-api/report-metrics-metric-api/) and
[Event API](https://docs.newrelic.com/docs/data-apis/ingest-apis/event-api/introduction-event-api/).
No New Relic agent or OpenTelemetry collector is needed alongside Home Assistant.

It is a fork of [kamaradclimber/datadog-integration-ha](https://github.com/kamaradclimber/datadog-integration-ha) 🙇
(itself a fork of https://github.com/tjschiffer/hassio-addons). The state-to-number conversion, unit
normalisation and label handling are unchanged; only the transport and data format were rewritten for New Relic.

## Installation

Through HACS, as a custom repository:

1. In HACS, open the menu → **Custom repositories**
1. Add `https://github.com/OWNER/newrelic-integration-ha` with category **Integration**
1. Install **New Relic Agentless** and restart Home Assistant
1. Go to **Settings → Devices & services → Add integration** and pick **New Relic agentless**

Or manually: copy `custom_components/newrelic_agentless` into your Home Assistant `config/custom_components/` and restart.

It uses a different domain (`newrelic_agentless`) from the Datadog version, so both can run side by side.

## Configuration

You'll be asked for:

- **License key** – an `INGEST - LICENSE` key (New Relic → your name → API keys). User keys (`NRAK-…`) are rejected.
  The key decides which account receives the data, so no account ID is needed.
- **Region** – `US`, `EU` or `JP`, matching your New Relic account's data center.
- **Environment** – sent as the `env` attribute on every metric and event. Suggested value: `prod`.

The key is checked when you submit the form.

## What gets sent

### Metrics

Every state change that can be turned into a number becomes a **gauge** named `hass.<domain>`, with the unit
appended when known, e.g. `hass.sensor_in_watt`, `hass.sensor_in_degree_celcius`, `hass.binary_sensor`.
Numeric attributes are sent too, as `entity = <entity_id>_attribute_<name>`.
Values that haven't changed are re-sent every ~4 minutes so graphs don't have gaps.

Attributes on every metric:

| Attribute | Example |
|---|---|
| `entity` | `sensor.living_room_temperature` |
| `friendly_name` | `Living room temperature` |
| `unit` | `degree_celcius` (when known) |
| `service` | `home-assistant` |
| `version` | Home Assistant version |
| `env` | from the config |
| `entity_label`, `device_label`, `area_label` | from Home Assistant labels |

Labels containing a colon become their own attribute: a label named `room:kitchen` becomes `room = 'kitchen'`.
New Relic allows one value per attribute, so several labels of the same kind are joined with commas
(`entity_label = 'critical,outdoor'`). Filter them with `LIKE`.

Example queries:

```sql
SELECT average(hass.sensor_in_watt) FROM Metric FACET friendly_name TIMESERIES SINCE 1 day ago
SELECT latest(hass.sensor_in_degree_celcius) FROM Metric WHERE area_label LIKE '%upstairs%' FACET entity
```

### Events

Home Assistant events that aren't turned into metrics (service calls, automations triggered, components
loaded, non-numeric state changes, …) are sent as custom events of type `HomeAssistantEvent`, with `title`,
`event_type`, `text` (the event as JSON, truncated to 4096 characters) and event-specific attributes such as
`domain`, `service`, `entity_id`, `new_state`, `name`.

```sql
SELECT count(*) FROM HomeAssistantEvent FACET event_type SINCE 1 day ago
SELECT timestamp, name, source FROM HomeAssistantEvent WHERE event_type = 'automation_triggered' SINCE 1 day ago
```

## Notes

- Every Home Assistant event is forwarded, which can add up on a busy instance. Keep an eye on ingest
  in New Relic's **Data management** page if you're near the free tier limit.
- New Relic accepts data asynchronously. If nothing shows up, run `SELECT * FROM NrIntegrationError SINCE 1 hour ago`.
- Data is batched and sent every 10 seconds, gzip-compressed, with batches split automatically to stay under the 1 MB limit.
  Failed batches are retried a few times on rate-limit or server errors, then dropped and logged.

## Development

```sh
pip install -r requirements.txt
python -m unittest discover -s custom_components/newrelic_agentless/tests -t .
```
