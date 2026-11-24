"""Render the README chart from the archived CPU pilot with Matplotlib."""
from collections import Counter
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

DIRECTORY = Path(__file__).resolve().parent
PATHS = {'direct': 'Direct to llama.cpp', 'fifo': 'FIFO gateway', 'priority': 'Priority gateway'}
OUTCOMES = {
    'completed': ('Completed', '#59dfc5'),
    'shed': ('Intentional 429', '#ffc46b'),
    'timeout': ('Timed out', '#eb91ab'),
}
BACKGROUND = '#101c28'
TEXT = '#eff5fa'
SECONDARY_TEXT = '#b1c1d0'


def render():
    report = json.loads((DIRECTORY / 'windows-cpu-pilot.json').read_text(encoding='utf-8'))
    high_rate = max(report['free_rates_rps'])
    counts = {}
    for name in PATHS:
        counts[name] = Counter()
        for trial in report['targets'][name]['trials']:
            if trial['free_rate_rps'] == high_rate:
                counts[name].update(trial['aggregate']['outcomes'])
        if counts[name].keys() - OUTCOMES.keys():
            raise ValueError(f'Uncharted outcomes for {name}: {counts[name]}')

    total = max(sum(path.values()) for path in counts.values())
    figure = plt.figure(figsize=(10.3, 3.7), dpi=150, facecolor=BACKGROUND)
    figure.text(0.046, 0.89, f'What happened to {total} requests?', fontsize=23, weight='bold', color=TEXT)
    figure.text(0.046, 0.80, 'Windows CPU pilot  ·  Higher offered load  ·  Two matched repeats',
                fontsize=10, color=SECONDARY_TEXT)
    axis = figure.add_axes((0.235, 0.24, 0.71, 0.48), facecolor=BACKGROUND)
    for index, name in enumerate(PATHS):
        offset = 0
        for outcome, (_, color) in OUTCOMES.items():
            count = counts[name][outcome]
            if count:
                axis.barh(index, count, left=offset, color=color, height=0.55)
                axis.text(offset + count / 2, index, str(count), ha='center', va='center',
                          color=BACKGROUND, fontsize=11, weight='bold')
                offset += count

    axis.set_xlim(0, total)
    axis.set_yticks(range(len(PATHS)), list(PATHS.values()), color=TEXT, fontsize=11)
    axis.invert_yaxis()
    axis.set_xticks([0, total // 4, total // 2, 3 * total // 4, total])
    axis.tick_params(axis='x', labelcolor=SECONDARY_TEXT, labelsize=9)
    axis.tick_params(length=0, pad=10)
    axis.set_axisbelow(True)
    axis.grid(axis='x', color='#334958', linewidth=0.65)
    for spine in axis.spines.values():
        spine.set_visible(False)

    figure.legend(handles=[Patch(facecolor=color, label=label) for label, color in OUTCOMES.values()],
                  loc='lower left', bbox_to_anchor=(0.036, 0.035), ncol=3, frameon=False,
                  fontsize=10, labelcolor=TEXT)
    output = DIRECTORY / 'figures' / 'cpu-overload.png'
    output.parent.mkdir(exist_ok=True)
    figure.savefig(output, facecolor=BACKGROUND)
    plt.close(figure)


if __name__ == '__main__':
    render()
