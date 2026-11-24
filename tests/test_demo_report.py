import unittest

from scripts.demo_report import render_html

PHASES = ('baseline', 'spike', 'recovery')


def sample(offered, completed, shed, ttft):
    return {'offered': offered, 'outcomes': {'completed': completed, 'shed': shed}, 'p95_ttft_s': ttft}


def report_fixture():
    targets = {}
    for name, free, paid in (
        ('direct', sample(72, 0, 0, None), sample(6, 4, 0, 0.201)),
        ('fifo', sample(72, 30, 42, 0.101), sample(6, 2, 4, 0.091)),
        ('priority', sample(72, 20, 52, 0.1), sample(6, 6, 0, 0.078)),
    ):
        steady = sample(1, 1, 0, 0.05)
        targets[name] = {
            'summary': dict.fromkeys(PHASES),
            'tier_summary': {
                'free': {'baseline': steady, 'spike': free, 'recovery': steady},
                'paid': {'baseline': steady, 'spike': paid, 'recovery': steady},
            },
        }
    return {
        'targets': targets, 'upstream': 'Fixture <two slots>', 'seed': 42, 'client_timeout_s': 1.5,
        'durations_s': (1, 3, 1), 'free_rates_rps': (4, 24, 4), 'paid_rates_rps': (2, 2, 2),
    }


class DemoReportTests(unittest.TestCase):
    def test_renders_phase_navigation_and_measured_spike_outcomes(self):
        page = render_html(report_fixture())

        for phase in PHASES:
            self.assertIn(f'<a href="#{phase}">{phase.title()}</a>', page)
            self.assertIn(f'<table aria-labelledby="{phase}-heading">', page)
        self.assertEqual(page.count('<th scope="row">'), 18)
        self.assertIn('3 s · 24 free and 2 paid requests per second', page)
        self.assertRegex(page, r'<dt>Paid completed · Priority</dt><dd[^>]*>6 / 6</dd>')
        self.assertRegex(page, r'<dt>Paid completed · FIFO</dt><dd[^>]*>2 / 6</dd>')
        self.assertRegex(page, r'<dt>Free shed · Priority</dt><dd[^>]*>52 / 72</dd>')
        self.assertRegex(page, r'0 / 72</td>\s*<td>0</td><td>72</td><td class="numeric">—</td>')
        self.assertIn('0.078 s', page)

    def test_escapes_metadata_and_needs_no_remote_assets_or_script(self):
        page = render_html(report_fixture())

        self.assertIn('Fixture &lt;two slots&gt;', page)
        self.assertNotIn('Fixture <two slots>', page)
        self.assertNotIn('<script', page)
        self.assertNotIn('<link', page)
        self.assertNotIn('https://', page)
