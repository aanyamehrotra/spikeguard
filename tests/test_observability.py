import json
from pathlib import Path
import re
import unittest

import httpx
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from gateway.app import Settings, create_app
from replay import PHASES
from replay_metrics import ReplayMetrics


DASHBOARD = Path(__file__).resolve().parents[1] / 'observability' / 'grafana' / 'dashboards' / 'replay.json'
METRIC_NAME = re.compile(r'\bspikeguard_[A-Za-z0-9_:]+\b')


class ObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_dashboard_queries_reference_exported_spikeguard_metrics(self):
        app = create_app(Settings(paid_slots=1, paid_queue_slots=1, adaptive=True))
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url='http://gateway') as client:
                gateway_metrics = (await client.get('/metrics')).raise_for_status().text

        replay_metrics = generate_latest(ReplayMetrics(PHASES).registry).decode('utf-8')
        exported = {
            sample.name
            for payload in (gateway_metrics, replay_metrics)
            for family in text_string_to_metric_families(payload)
            for sample in family.samples
        }
        dashboard = json.loads(DASHBOARD.read_text(encoding='utf-8'))
        for panel in dashboard['panels']:
            for target in panel['targets']:
                with self.subTest(panel=panel['title'], query=target['refId']):
                    missing = set(METRIC_NAME.findall(target['expr'])) - exported
                    self.assertEqual(missing, set())
