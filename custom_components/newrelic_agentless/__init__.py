import logging
from functools import partial
from typing import Optional, Tuple, Any
import datetime
import re
import dateutil.parser
import json
import orjson
import asyncio
import copy
import math
from collections.abc import Callable

from homeassistant.helpers.aiohttp_client import async_get_clientsession

from homeassistant.const import Platform, EVENT_STATE_CHANGED, MATCH_ALL, EVENT_HOMEASSISTANT_STOP
from homeassistant.helpers.device_registry import EVENT_DEVICE_REGISTRY_UPDATED
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import label_registry as lr
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.components.automation import EVENT_AUTOMATION_TRIGGERED
from homeassistant.const import __version__ as HAVERSION
import homeassistant.const
from homeassistant.core import HomeAssistant, Event, EventStateChangedData, State, callback
from homeassistant.config_entries import ConfigEntry
from .const import DOMAIN, CONF_LICENSE_KEY, CONF_REGION, CONF_ENV, DEFAULT_REGION, EVENT_TYPE
from .newrelic_client import NewRelicClient, tags_to_attributes, truncate
from homeassistant.helpers.state import state_as_number
from homeassistant.components.sensor.const import SensorDeviceClass

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    if entry.entry_id not in hass.data[DOMAIN]:
        hass.data[DOMAIN][entry.entry_id] = {}

    # will make sure async_setup_entry from sensor.py is called
    await hass.config_entries.async_forward_entry_setups(entry, [Platform.SENSOR])

    # subscribe to config updates
    entry.async_on_unload(entry.add_update_listener(update_entry))
    
    # we'll use that dictionary to store for each metric the last value and timestamp of last last event
    constant_emitter = ConstantMetricEmitter()

    creds = entry.data
    client = NewRelicClient(
            async_get_clientsession(hass),
            creds[CONF_LICENSE_KEY],
            creds.get(CONF_REGION, DEFAULT_REGION),
    )

    metrics_queue = asyncio.Queue(maxsize=0)
    @callback
    def metrics_dequeue_callback(now) -> None:
        entry.async_create_background_task(hass, send_metrics_loop(metrics_queue, client, constant_emitter), name="dequeue metrics to be sent to new relic", eager_start=True)
    entry.async_on_unload(async_track_time_interval(hass, metrics_dequeue_callback, datetime.timedelta(seconds=10)))

    event_listener=partial(full_event_listener, entry.data, hass, constant_emitter, metrics_queue)
    unsubscribe = hass.bus.async_listen(EVENT_STATE_CHANGED, event_listener)
    hass.data[DOMAIN][entry.entry_id]["unsubscribe_handler"] = unsubscribe

    events_queue = asyncio.Queue(maxsize=0)
    @callback
    def events_dequeue_callback(now) -> None:
        entry.async_create_background_task(hass, send_events_loop(events_queue, client), name="dequeue events to be sent to new relic", eager_start=True)
    entry.async_on_unload(async_track_time_interval(hass, events_dequeue_callback, datetime.timedelta(seconds=10)))

    all_event_listener=partial(full_all_event_listener, entry.data, events_queue)
    unsubscribe_all_events = hass.bus.async_listen(MATCH_ALL, all_event_listener)
    hass.data[DOMAIN][entry.entry_id]["unsubscribe_all_event_handler"] = unsubscribe_all_events

    return True

EVENTS_BATCH_SIZE = 500
METRICS_BATCH_SIZE = 1024


def _drain(queue: asyncio.Queue, limit: int) -> list:
    items = []
    while len(items) < limit and not queue.empty():
        items.append(queue.get_nowait())
    return items


# dequeue events and send them by batch to the New Relic Event API
async def send_events_loop(queue, client: NewRelicClient):
    _LOGGER.debug("Starting dequeuing from events queue")
    while not queue.empty():
        events = _drain(queue, EVENTS_BATCH_SIZE)
        try:
            _LOGGER.debug(f"Sending {len(events)} events to New Relic")
            await client.send_events(events)
        except Exception as e:
            _LOGGER.error(f"Error sending {len(events)} events to New Relic, dropping them: {e}")
    _LOGGER.debug("Events queue is now empty, waiting for next run")


# dequeue metrics and send them by batch to the New Relic Metric API
async def send_metrics_loop(queue, client: NewRelicClient, constant_emitter):
    _LOGGER.debug("Checking if time to flush old series")
    if constant_emitter.should_flush():
        old_series = await constant_emitter.flush_old_series()
        for serie in old_series:
            await queue.put(serie)
    _LOGGER.debug("Starting dequeuing from metrics queue")
    while not queue.empty():
        metrics = _drain(queue, METRICS_BATCH_SIZE)
        try:
            _LOGGER.debug(f"Sending {len(metrics)} metrics to New Relic")
            await client.send_metrics(metrics)
        except Exception as e:
            _LOGGER.error(f"Error sending {len(metrics)} metrics to New Relic, dropping them: {e}")
    _LOGGER.debug("Metrics queue is now empty, waiting for next run")


async def update_entry(hass, entry):
    """
    This method is called when options are updated
    We trigger the reloading of entry (that will eventually call async_unload_entry)
    """
    _LOGGER.debug("update_entry method called")
    # will make sure async_setup_entry from sensor.py is called
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """This method is called to clean all sensors before re-adding them"""
    _LOGGER.debug("async_unload_entry method called")
    unload_ok = await hass.config_entries.async_unload_platforms(
        entry, [Platform.SENSOR]
    )
    if "unsubscribe_handler" in hass.data[DOMAIN][entry.entry_id]:
        hass.data[DOMAIN][entry.entry_id]["unsubscribe_handler"]()
    if "unsubscribe_all_event_handler" in hass.data[DOMAIN][entry.entry_id]:
        hass.data[DOMAIN][entry.entry_id]["unsubscribe_all_event_handler"]()
    hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok

PREFIX = "hass"


class ConstantMetricEmitter:
    def __init__(self):
        self.last_sent = dict()
        self.last_flush = int(datetime.datetime.now().timestamp())
        self.flush_interval = 250
        self.mutex = asyncio.Lock()

    async def record_last_sent(self, metric: dict) -> None:
        async with self.mutex:
            key = (metric["name"], tuple(sorted(metric["attributes"].items())))
            self.last_sent[key] = copy.deepcopy(metric)

    async def flush_old_series(self) -> list[dict]:
        async with self.mutex:
            now = int(datetime.datetime.now().timestamp())
            old_series = []
            for metric_id in self.last_sent:
                serie = self.last_sent[metric_id]
                # metric timestamps are in milliseconds
                if serie["timestamp"] < (now - self.flush_interval) * 1000:
                    serie["timestamp"] = now * 1000
                    old_series.append(copy.deepcopy(serie))
            self.last_flush = now
            _LOGGER.info(f"Flushing {len(old_series)} old metrics (out of {len(self.last_sent)}) to make sure we have regular points")
            return old_series

    def should_flush(self) -> bool:
        now = int(datetime.datetime.now().timestamp())
        return self.last_flush < now - self.flush_interval


def ignore_by_entity_id(entity_id: str) -> bool:
    if re.match(".+_version$", entity_id):
        return True
    if re.match("^person.", entity_id):
        return True
    if re.match("event.repair", entity_id):
        return True
    if re.match(".+airing_now_on_.+", entity_id):
        return True
    if re.match("sensor.time", entity_id):
        return True
    if re.match("sensor.date_time", entity_id):
        return True
    if re.match(".+_friendly_?name", entity_id):
        return True
    if re.match(".+_unitofmeasurement", entity_id):
        return True
    if re.match(".+_deviceclass", entity_id):
        return True
    if re.match(".+_stateclass", entity_id):
        return True
    if re.match(".+_icon", entity_id):
        return True
    if re.match(".+_mode", entity_id):
        return True
    if re.match(".+_entity_?picture", entity_id):
        return True
    if re.match(".+_metertypename", entity_id):
        return True
    if re.match(".+_restored", entity_id):
        return True
    # powercalc attributes
    if re.match(".+_device_power_attribute_.+", entity_id):
        return True
    if re.match(".+_attribute_(source|sourcedomain|sourceentity|energysensorentityid|integration|calculationmode|priceentity|energyentity|status|accesstoken|attribution|devicename|deviceid|batterytypeandquantity|batterytype|access_token|energy_entity|nickname|period_key|_weather|country_code)", entity_id):
        return True
    if re.match(".+unit", entity_id):
        return True
    if entity_id == "":
        return True
    if re.match(".+_name", entity_id):
        # example: sensor.smart_switch_6_electric_consumed_w_attribute_meter_type_name
        return True

    return False

def extract_states(event: Event[EventStateChangedData]) -> list[Tuple[str,Optional[Tuple[str, Callable]],float]]:
    new_state = event.data["new_state"]
    states = []
    if new_state is None:
        return states
    main_state = _extract_state(new_state, new_state.entity_id, new_state.state, True)
    unit = _extract_unit(new_state, new_state.entity_id)
    if main_state is not None:
        states.append((new_state.entity_id, unit, main_state))
    for key, value in new_state.attributes.items():
        fake_id = new_state.entity_id + "_attribute_" + sanitize(key)
        parsed_value = _extract_state(new_state, fake_id, value, False)
        if parsed_value:
            states.append((fake_id, None, parsed_value))
    return states

def sanitize(key: str) -> str:
    valid = set('0123456789abcdefghijklmnopqrstuvwxyz_')
    return ''.join(filter(lambda x: x in valid, key))

def ident(x):
    return x


UNITS = {
        "%": ("percent", ident),
        "°": ("degree", ident), # angles
        "€": ("€", ident),
        "€/kWh": ("euro_per_kWh", ident),
        "A": ("ampere", ident),
        "B": ("byte", ident),
        "°C": ("degree_celcius", ident),
        "c€": ("€", lambda x: x/100),
        "c€/kWh": ("euro_per_kWh", lambda x: x/100),
        "°C.day": ("degree_day", ident),
        "clients": ("clients", ident),
        "cm": ("meter", lambda x: x/100),
        "EUR": ("euro", ident),
        "EUR/kWh": ("eur_per_kWh", ident),
        "gCO2eq": ("gram", ident),
        "h": ("s", lambda x: x*3600),
        "hPa": ("pascal", lambda x: x*100),
        "Hz": ("hertz", ident),
        "kg": ("gram", lambda x: x*1000),
        "kgCO2": ("gram", lambda x: x*1000),
        "km": ("meter", lambda x: x*1000),
        "km/h": ("km_per_hour", ident),
        "kW": ("watt", lambda x: x*1000),
        "kWh": ("kWh", ident),
        "L": ("liter", ident),
        "lqi": ("lqi", ident),
        "lx": ("lux", ident),
        "m": ("meter", ident),
        "m³": ("cubic_meter", ident),
        "mA": ("ampere", lambda x: x/1000),
        "min": ("second", lambda x: x*60),
        "mm": ("meter", lambda x: x/1000),
        "m/s": ("meter_per_second", ident),
        "ms": ("second", lambda x: x/1000),
        "R/min": ("rpm", ident),
        "rpm": ("rpm", ident),
        "s": ("second", ident),
        "V": ("volt", ident),
        "W": ("watt", ident),
        "Wh": ("kWh", lambda x: x/1000),
}


def _extract_unit(new_state: State, entity_id: str) -> Optional[Tuple[str, Callable]]:
    if "unit_of_measurement" in new_state.attributes:
        unit = new_state.attributes["unit_of_measurement"]
        # we only convert units that are single character, to increase readability
        resp = UNITS.get(unit, None)
        if resp is None:
            return None
        unit_str, convert_func = resp
        if unit_str == "":
            _LOGGER.warning(f"Unit {unit} for {entity_id} cannot be sanitized properly, open a bug to the maintainer of this integration")
            return None
        return unit_str.lower(), convert_func
    return None

# returns None if state cannot be convert to float and thus event should be ignored for metrics
def _extract_state(new_state: State, entity_id: str, value: Any, main_state: bool) -> Optional[float]:
    # if it already a number, that's the easy case
    if isinstance(value, (float, int)):
        return value
    if value is None:
        return None
    if isinstance(value, (list, dict, set)):
        return None
    if isinstance(value, datetime.datetime):
        return value.timestamp()
    if isinstance(value, tuple):
        if entity_id in ['hs_color', 'rgb_color', 'xy_color']:
            # ignore multivalue colors
            return
        if re.match("^light.wled_.+color", entity_id ):
            # ignore multivalue colors
            return
        _LOGGER.warning(f"Hard to convert value {value} which is a tuple to a single value (entity: {entity_id})")
        return
    # let's ignore "known" string values
    if str(value).lower() in ["unavailable", "unknown", "info", "warn", "debug", "error", "on/off", "off/on", "restore", "stop", "opening", "", "scene_mode", "sunny", "cloud", "partlycloudy", "brightness"]:
        return None

    # we can treat timestamps
    if "device_class" in new_state.attributes and new_state.attributes["device_class"] == SensorDeviceClass.TIMESTAMP and main_state:
        try:
            try:
                timestamp = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
            except ValueError:
                timestamp = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z").timestamp()
            return timestamp
        except ValueError:
            _LOGGER.debug(f"Unable to parse {value} as a timestamp")

    if re.match(".+_attribute_device_class$", entity_id):
        return None

    if re.match(".+_attribute_state_class$", entity_id):
        return None

    if re.match("^input_datetime.+", new_state.entity_id) and new_state.attributes["timestamp"]:
        return new_state.attributes["timestamp"]

    # we can treat things that look like timestamp
    if re.match("20..-..-..T..:..:...+" ,value):
        try:
            timestamp = dateutil.parser.parse(value).timestamp()
            return timestamp
        except ValueError:
            _LOGGER.debug(f"Unable to parse {value} as a timestamp, even if it looks like one")
    if re.match("20..-..-.." ,value):
        try:
            timestamp = dateutil.parser.parse(value).timestamp()
            return timestamp
        except ValueError:
            _LOGGER.debug(f"Unable to parse {value} as a timestamp, even if it looks like one")

    # some values can reasonnably be converted to numeric value
    if value.lower() in ["unprotected", "dead", "disabled", "inactive", "unlock", "off", "far", "down", "false", "none"]:
        return 0
    if value.lower() in ["alive", "ready", "enabled", "pending", "lock", "on", "near", "up"]:
        return 1

    # looks like a ssid
    if re.match("..:..:..:..:..:..", value):
        return None

    # deal with entities whose state are a list of options
    for key in ["operation_list", "options"]:
        if key in new_state.attributes and value in new_state.attributes[key]:
            return None

    stubbed_state = State(entity_id, value[:250])
    try:
        number = state_as_number(stubbed_state)
        return number
    except Exception as e:
        # we cannot treat this kind of event

        if ignore_by_entity_id(entity_id):
            return None

        _LOGGER.debug(f"Cannot treat this state changed event: {entity_id} to convert to metric. Error was: %s", e)
        return None

def additional_tags(hass, new_state) -> list[str]:
    """
    Adds new tags if relevant based on labels of the entity
    """
    tags = []
    label_registry = lr.async_get(hass)
    entity_registry = er.async_get(hass)
    entity = entity_registry.async_get(new_state.entity_id)
    if entity is not None:
        for label_id in entity.labels:
            label_name = label_id_to_name(label_registry, label_id)
            if ":" in label_name:
                tags.append(label_name)
            else:
                tags.append(f"entity_label:{label_name}")
        _LOGGER.debug(f"Added {len(entity.labels)} labels from entity")
        area_id = None
        device_registry = dr.async_get(hass)
        if entity.device_id is not None:
            device = device_registry.async_get(entity.device_id)
            if device is not None:
                for label_id in device.labels:
                    label_name = label_id_to_name(label_registry, label_id)
                    if ":" in label_name:
                        tags.append(label_name)
                    else:
                        tags.append(f"device_label:{label_name}")
                area_id = device.area_id
                _LOGGER.debug(f"Added {len(device.labels)} labels from device")
        if entity.area_id is not None:
            area_id = entity.area_id
        if area_id is not None:
            area_registry = ar.async_get(hass)
            # area registry method access is not consistent, it should be async_get
            area = area_registry.async_get_area(area_id)
            if area is not None:
                for label_id in area.labels:
                    label_name = label_id_to_name(label_registry, label_id)
                    if ":" in label_name:
                        tags.append(label_name)
                    else:
                        tags.append(f"area_label:{label_name}")
                _LOGGER.debug(f"Added {len(area.labels)} labels from area")
    return tags


async def full_event_listener(creds: dict, hass, constant_emitter: ConstantMetricEmitter, metrics_queue, event: Event[EventStateChangedData]):
    try:
        await unsafe_full_event_listener(creds, hass, constant_emitter, metrics_queue, event)
    except Exception:
        _LOGGER.exception("An error occured in event_state_changed callback")

async def unsafe_full_event_listener(creds: dict, hass, constant_emitter: ConstantMetricEmitter, metrics_queue, event: Event[EventStateChangedData]):
    new_state = event.data["new_state"]
    if new_state is None:
        _LOGGER.warning(f"This event has no new state, isn't it strange?. Event is {event}")
        return
    domain = new_state.domain if new_state.domain else new_state.entity_id.split(".")[0]
    unitless_metric_name = f"{PREFIX}.{domain}".replace(" ", "_")
    values = extract_states(event)
    friendly_name = new_state.attributes.get("friendly_name")
    if len(values) == 0:
        return
    common_tags = additional_tags(hass, new_state)
    for (name, unit, value) in values:
        tags = common_tags + [f"entity:{name}", "service:home-assistant", f"version:{HAVERSION}", f"env:{creds[CONF_ENV]}"]
        unit_name = None
        if unit is not None:
            unit_name, conversion = unit
            metric_name = f"{PREFIX}.{domain}".replace(" ", "_") + "_in_" + unit_name
            _LOGGER.debug(f"Using {metric_name} instead of the unitless metric name")
            value = conversion(value)
        else:
            metric_name = unitless_metric_name
        if friendly_name:
            tags.append(f"friendly_name:{friendly_name}")
        timestamp = new_state.last_changed
        if isinstance(value, bool):
            value = int(value)
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            # NaN / inf cannot be encoded as JSON numbers
            continue
        if unit_name is not None:
            tags.append(f"unit:{unit_name}")
        metric_serie = build_metric(metric_name, value, timestamp, tags)
        await metrics_queue.put(metric_serie)
        await constant_emitter.record_last_sent(metric_serie)

async def full_all_event_listener(creds: dict, events_queue, event: Event):
    try:
        await unsafe_full_all_event_listener(creds, events_queue, event)
    except Exception:
        _LOGGER.exception("An error occured in the event listener")

async def unsafe_full_all_event_listener(creds: dict, events_queue, event: Event):
    if event.event_type == "state_changed":
        if len(extract_states(event)) > 0:
            # those events will be converted to metric, no need to double send them
            # in addition we expect events about "metrics" to change more often than the others
            return

    tags = ["service:home-assistant", f"version:{HAVERSION}", f"event_type:{event.event_type}", f"env:{creds[CONF_ENV]}"]
    if event.event_type == homeassistant.const.EVENT_STATE_CHANGED:
        text=json.dumps(orjson.loads(orjson.dumps(event.json_fragment)), default=str)
    else:
        text=json.dumps(event.as_dict(), default=str)
    title, event_specific_tags = generate_message(event)
    await events_queue.put(build_event(title, text, event.time_fired, tags + event_specific_tags))


# Attributes New Relic reserves or that we set ourselves: never let a tag override them.
RESERVED_ATTRIBUTES = {"eventType", "timestamp", "accountId", "appId", "entity.guid", "entity.name", "entity.type", "title", "text"}


def build_metric(name: str, value: float, timestamp: datetime.datetime, tags: list[str]) -> dict:
    """Build one gauge datapoint in New Relic Metric API format."""
    attributes = {k: v for k, v in tags_to_attributes(tags).items() if k not in RESERVED_ATTRIBUTES}
    return {
        "name": name,
        "type": "gauge",
        "value": value,
        "timestamp": int(timestamp.timestamp() * 1000),
        "attributes": attributes,
    }


def build_event(title: str, text: str, time_fired: datetime.datetime, tags: list[str]) -> dict:
    """Build one custom event in New Relic Event API format."""
    event = {k: truncate(v) for k, v in tags_to_attributes(tags).items() if k not in RESERVED_ATTRIBUTES}
    event.update({
        "eventType": EVENT_TYPE,
        "timestamp": int(time_fired.timestamp() * 1000),
        "title": title,
        "text": truncate(text),
    })
    return event



def generate_message(event: Event) -> Tuple[str, list[str]]:
    tags = []
    def enrich(data_key):
        if data_key in event.data:
            tags.append(f"{data_key}:{event.data[data_key]}")

    tags.append(f"origin:{event.origin}")
    tags.append(f"user_id:{event.context.user_id}")
    tags.append(f"parent_id:{event.context.parent_id}")
    if event.event_type == homeassistant.const.EVENT_CALL_SERVICE:
        enrich("domain")
        enrich("service")
        return ("Called service", tags)
    elif event.event_type == homeassistant.const.EVENT_COMPONENT_LOADED:
        enrich("component")
        return ("Loaded component", tags)
    elif event.event_type == homeassistant.const.EVENT_SERVICE_REGISTERED:
        enrich("domain")
        enrich("service")
        return ("Registered a service", tags)
    elif event.event_type == homeassistant.const.EVENT_STATE_CHANGED:
        if "old_state" in event.data and event.data["old_state"] is not None:
            tags.append(f"entity_id:{event.data["old_state"].entity_id}")
        if "new_state" in event.data and event.data["new_state"] is not None:
            tags.append(f"entity_id:{event.data["new_state"].entity_id}")
        state_tag, extra_tags = build_state_tag(event)
        if state_tag is not None:
            tags.append(f"new_state:{state_tag}")
        for t in extra_tags:
            tags.append(t)
        return ("State changed", list(set(tags)))
    elif event.event_type == EVENT_DEVICE_REGISTRY_UPDATED:
        enrich("device_id")
        enrich("action")
        return ("device_registry_updated", tags)
    elif event.event_type == EVENT_AUTOMATION_TRIGGERED:
        enrich("source")
        enrich("entity_id")
        enrich("name")
        return ("automation_triggered", tags)
    else:
        return (str(event.event_type), tags)

def build_state_tag(event) -> Tuple[Optional[str], list[str]]:
    if "new_state" not in event.data or event.data["new_state"] is None:
        return (None, [])
    new_state = event.data["new_state"]
    if "device_class" in new_state.attributes and new_state.attributes["device_class"] == SensorDeviceClass.TIMESTAMP:
        return (None, [])
    for key in ["operation_list", "options"]:
        if key in new_state.attributes and new_state.state in new_state.attributes[key]:
            return (new_state.state, [f"state_type:{key}"])
    if "icon" in new_state.attributes and re.match(".*clock.*", new_state.attributes["icon"]):
        return (None, [])
    if new_state.state == "":
        return (None, [])
    return (new_state.state, [])

def label_id_to_name(label_registry, label_id) -> str:
    label = label_registry.async_get_label(label_id)
    if label is not None:
        return label.name
    return label_id
