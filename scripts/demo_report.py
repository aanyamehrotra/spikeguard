"""Render a self-contained HTML view of the measured demo results."""
from html import escape

PATH_LABELS = {'direct': 'Direct', 'fifo': 'FIFO gateway', 'priority': 'Priority gateway'}
STYLES = """
  :root {
    color-scheme: dark;
    --background: #101c28;
    --panel: #172635;
    --border: #354c60;
    --text: #eef5fa;
    --muted: #b5c5d4;
    --complete: #59dfc5;
    --shed: #ffc46b;
    --error: #eb91ab;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--background);
    color: var(--text);
    font: 1rem/1.5 system-ui, sans-serif;
  }
  main { width: min(100% - 2rem, 67rem); margin: 0 auto; padding: 3rem 0; }
  h1, h2, p, dl { margin: 0; }
  h1 { margin-bottom: .65rem; font-size: clamp(2rem, 5vw, 3.25rem); line-height: 1.1; }
  h2 { font-size: 1.4rem; }
  p { color: var(--muted); }
  header p + p { margin-top: .45rem; }
  .setup { font-size: .9rem; }
  .spotlight { margin: 2rem 0; }
  .spotlight h2 { margin-bottom: .75rem; }
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 12rem), 1fr));
    gap: .75rem;
  }
  .cards div, .phase { border: 1px solid var(--border); border-radius: .8rem; background: var(--panel); }
  .cards div { padding: .85rem 1rem; }
  .cards dt { color: var(--muted); font-size: .9rem; }
  .cards dd { margin: .2rem 0 0; font-size: 1.8rem; font-weight: 700; font-variant-numeric: tabular-nums; }
  .cards .paid { color: var(--complete); }
  .cards .free { color: var(--shed); }
  nav { display: flex; flex-wrap: wrap; gap: .55rem; margin: 0 0 1.5rem; }
  nav a { padding: .35rem .8rem; border: 1px solid var(--border); border-radius: .5rem; }
  a { color: var(--complete); text-underline-offset: .2em; }
  a:hover { color: var(--text); }
  :focus-visible { outline: 2px solid var(--complete); outline-offset: 3px; }
  .phase { margin-bottom: 1rem; padding: 1rem; scroll-margin-top: .7rem; }
  .phase p { margin: .15rem 0 .65rem; font-size: .9rem; }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: .9rem; font-variant-numeric: tabular-nums; }
  th, td { padding: .55rem .5rem; border-bottom: 1px solid var(--border); text-align: right; }
  tbody tr:last-child > * { border-bottom: 0; }
  tr > :first-child { text-align: left; padding-left: 0; }
  tr > :last-child { padding-right: 0; }
  thead th { color: var(--muted); font-weight: 600; }
  tbody th { font-weight: 600; }
  .tier { display: block; color: var(--muted); font-size: .8em; font-weight: 400; }
  .numeric { white-space: nowrap; }
  .mix { min-width: 8rem; width: 28%; }
  .bar { display: flex; height: .5rem; overflow: hidden; border-radius: 1rem; background: var(--border); }
  .segment { flex-basis: 0; }
  .completed { background: var(--complete); }
  .shed { background: var(--shed); }
  .errors { background: var(--error); }
  footer { padding: .25rem 0; font-size: .85rem; }
  @media (max-width: 44rem) { .mix { display: none; } }
  @media (max-width: 30rem) {
    main { width: calc(100% - 1.25rem); padding: 1.6rem 0; }
    .phase { padding: .7rem; }
    table { font-size: .76rem; }
    th, td { padding: .5rem .25rem; }
  }
"""


def render_rows(targets, phase):
    rows = []
    for path, result in targets.items():
        label = escape(PATH_LABELS.get(path, path))
        for tier, phases in result['tier_summary'].items():
            sample = phases[phase]
            offered = sample['offered']
            completed = sample['outcomes'].get('completed', 0)
            shed = sample['outcomes'].get('shed', 0)
            errors = offered - completed - shed
            ttft = sample['p95_ttft_s']
            latency = f'{ttft:.3f} s' if ttft is not None else '—'
            segments = ''.join(
                f'<span class="segment {style}" style="flex-grow:{count:d}"></span>'
                for style, count in (('completed', completed), ('shed', shed), ('errors', errors)) if count
            )
            rows.append(f"""<tr>
              <th scope="row">{label}<span class="tier">{escape(tier.title())}</span></th>
              <td class="mix" aria-hidden="true"><div class="bar">{segments}</div></td>
              <td class="numeric">{completed:d} / {offered:d}</td>
              <td>{shed:d}</td><td>{errors:d}</td><td class="numeric">{latency}</td>
            </tr>""")
    return '\n'.join(rows)


def render_phase(targets, phase, *, duration, free_rate, paid_rate):
    name = escape(phase.title())
    anchor = escape(phase)
    return f"""<section class="phase" id="{anchor}" aria-labelledby="{anchor}-heading">
      <h2 id="{anchor}-heading">{name}</h2>
      <p>{duration:g} s · {free_rate:g} free and {paid_rate:g} paid requests per second</p>
      <div class="table-wrap" tabindex="0" role="region" aria-label="{name} request outcomes">
        <table aria-labelledby="{anchor}-heading">
          <thead><tr>
            <th scope="col">Path and tier</th>
            <th class="mix" scope="col" aria-hidden="true">Outcome mix</th>
            <th scope="col">Completed</th><th scope="col">Shed</th>
            <th scope="col">Errors</th><th scope="col">p95 TTFT</th>
          </tr></thead>
          <tbody>{render_rows(targets, phase)}</tbody>
        </table>
      </div>
    </section>"""


def render_html(report):
    targets = report['targets']
    phases = targets['direct']['summary']
    spotlight = []
    for label, path, tier, outcome, style in (
        ('Paid completed · Priority', 'priority', 'paid', 'completed', 'paid'),
        ('Paid completed · FIFO', 'fifo', 'paid', 'completed', ''),
        ('Free shed · Priority', 'priority', 'free', 'shed', 'free'),
    ):
        stats = targets[path]['tier_summary'][tier]['spike']
        count = stats['outcomes'].get(outcome, 0)
        spotlight.append(f'<div><dt>{label}</dt><dd class="{style}">{count:d} / {stats["offered"]:d}</dd></div>')

    links = (
        f'<a href="#{escape(phase)}">{escape(phase.title())}</a>'
        for phase in phases
    )
    sections = (
        render_phase(targets, phase, duration=duration, free_rate=free_rate, paid_rate=paid_rate)
        for phase, duration, free_rate, paid_rate in zip(
            phases, report['durations_s'], report['free_rates_rps'], report['paid_rates_rps'], strict=True,
        )
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SpikeGuard · Demo results</title>
  <style>{STYLES}</style>
</head>
<body>
  <main>
    <header>
      <h1>SpikeGuard demo results</h1>
      <p>Direct, FIFO, and priority paths replayed the same traffic against a local streaming fixture.</p>
      <p class="setup">{escape(report['upstream'])} · Seed {report['seed']:d} ·
        {report['client_timeout_s']:g} s client deadline</p>
    </header>
    <section class="spotlight" aria-labelledby="spotlight-heading">
      <h2 id="spotlight-heading">During the spike</h2>
      <dl class="cards">{''.join(spotlight)}</dl>
    </section>
    <nav aria-label="Replay phases">{' '.join(links)}</nav>
    {''.join(sections)}
    <footer>
      <p>Completed is shown as completed / offered. Shed counts intentional 429 responses. Errors cover other
        failures. p95 TTFT measures time to first assistant content among completed streams. A dash indicates
        zero completions.</p>
    </footer>
  </main>
</body>
</html>
"""
