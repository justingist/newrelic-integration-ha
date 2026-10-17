from os import path
import sys
sys.path.append(".")
sys.path.append(path.dirname(path.dirname(__file__)))

import asyncio
import datetime
import gzip
import json
import unittest
from unittest.mock import patch

import aiohttp
from aiohttp import web

from homeassistant.core import Event, State
import custom_components.newrelic_agentless as integration
from custom_components.newrelic_agentless import build_event, build_metric, ConstantMetricEmitter
from custom_components.newrelic_agentless.newrelic_client import (
    NewRelicClient, InvalidAuth, tags_to_attributes, truncate,
)


class TestConversion(unittest.TestCase):
    def test_tags_to_attributes_merges_repeated_keys(self):
        attrs = tags_to_attributes(["entity_label:b", "entity_label:a", "room:kitchen", "flag", "url:http://x"])
        self.assertEqual(attrs, {"entity_label": "a,b", "room": "kitchen", "flag": "true", "url": "http://x"})

    def test_build_metric(self):
        ts = datetime.datetime(2026, 10, 17, 12, 0, tzinfo=datetime.timezone.utc)
        m = build_metric("hass.sensor_in_watt", 42.5, ts, ["entity:sensor.plug", "env:prod", "timestamp:evil"])
        self.assertEqual(m["name"], "hass.sensor_in_watt")
        self.assertEqual(m["type"], "gauge")
        self.assertEqual(m["value"], 42.5)
        self.assertEqual(m["timestamp"], int(ts.timestamp() * 1000))
        self.assertEqual(m["attributes"], {"entity": "sensor.plug", "env": "prod"})

    def test_build_event_truncates(self):
        ts = datetime.datetime(2026, 10, 17, 12, 0, tzinfo=datetime.timezone.utc)
        e = build_event("Called service", "x" * 10000, ts, ["event_type:call_service", "domain:light"])
        self.assertEqual(e["eventType"], "HomeAssistantEvent")
        self.assertEqual(len(e["text"]), 4096)
        self.assertEqual(e["domain"], "light")
        self.assertEqual(e["event_type"], "call_service")
        self.assertEqual(truncate("short"), "short")


class TestListener(unittest.IsolatedAsyncioTestCase):
    async def test_state_change_becomes_metric_with_unit(self):
        state = State("sensor.plug_power", "1.5", {"unit_of_measurement": "kW", "friendly_name": "Plug"},
                      last_changed=datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.timezone.utc))
        event = Event("state_changed", {"entity_id": "sensor.plug_power", "old_state": None, "new_state": state})
        queue = asyncio.Queue()
        emitter = ConstantMetricEmitter()
        with patch.object(integration, "additional_tags", return_value=["area_label:downstairs"]):
            await integration.unsafe_full_event_listener({"env": "test"}, None, emitter, queue, event)
        metric = queue.get_nowait()
        self.assertEqual(metric["name"], "hass.sensor_in_watt")
        self.assertEqual(metric["value"], 1500)
        self.assertEqual(metric["attributes"]["unit"], "watt")
        self.assertEqual(metric["attributes"]["friendly_name"], "Plug")
        self.assertEqual(metric["attributes"]["area_label"], "downstairs")
        self.assertEqual(metric["attributes"]["env"], "test")

        # old points get re-emitted with a fresh timestamp, without mutating what was queued
        emitter.flush_interval = 0
        flushed = await emitter.flush_old_series()
        self.assertEqual(len(flushed), 1)
        self.assertGreater(flushed[0]["timestamp"], metric["timestamp"])


class TestClient(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.received = []

        async def handler(request):
            if request.headers.get("Api-Key") != "good":
                return web.Response(status=403, text="bad key")
            assert request.headers.get("Content-Encoding") == "gzip"
            raw = await request.read()  # aiohttp transparently decompresses gzip bodies
            body = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
            self.received.append((request.path, body))
            return web.Response(status=202 if request.path.startswith("/metric") else 200, text="{}")

        app = web.Application(client_max_size=10 * 1024 * 1024)
        app.router.add_post("/{tail:.*}", handler)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.urls = {"metrics": f"http://127.0.0.1:{port}/metric/v1",
                     "events": f"http://127.0.0.1:{port}/v1/accounts/events"}
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.session.close()
        await self.runner.cleanup()

    def client(self, key):
        c = NewRelicClient(self.session, key, "EU")
        c._urls = self.urls
        return c

    async def test_metrics_and_events_payload_shape(self):
        c = self.client("good")
        await c.validate()
        await c.send_metrics([{"name": "m", "type": "gauge", "value": 1, "timestamp": 1, "attributes": {}}])
        await c.send_events([{"eventType": "HomeAssistantEvent", "title": "t"}])
        self.assertEqual(self.received[1], ("/metric/v1", [{"metrics": [{"name": "m", "type": "gauge", "value": 1, "timestamp": 1, "attributes": {}}]}]))
        self.assertEqual(self.received[2], ("/v1/accounts/events", [{"eventType": "HomeAssistantEvent", "title": "t"}]))

    async def test_bad_key(self):
        with self.assertRaises(InvalidAuth):
            await self.client("bad").validate()

    async def test_large_batches_are_split(self):
        import random, string
        rnd = random.Random(0)
        events = [{"eventType": "E", "text": "".join(rnd.choices(string.ascii_letters, k=4000))} for _ in range(600)]
        await self.client("good").send_events(events)
        self.assertGreater(len(self.received), 1)
        self.assertEqual(sum(len(b) for _, b in self.received), 600)


if __name__ == "__main__":
    unittest.main()
