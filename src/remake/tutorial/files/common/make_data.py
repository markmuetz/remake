"""Make the tutorial's raw data: a year of daily temperatures for each weather
station, written to data/raw/<site>/<year>.csv. Deterministic — run it as
often as you like. About one day in twenty has a missing reading (NA).
"""
import math
import random
from pathlib import Path

SITES = {'aberdeen': 8.5, 'cambridge': 11.0, 'durham': 9.5, 'exeter': 11.5}
YEARS = [2020, 2021, 2022, 2023]

for site, mean in SITES.items():
    for year in YEARS:
        rng = random.Random(f'{site}-{year}')
        lines = ['day,temp_c']
        for day in range(1, 366):
            if rng.random() < 0.05:
                lines.append(f'{day},NA')
                continue
            seasonal = -6 * math.cos(2 * math.pi * (day - 15) / 365)
            lines.append(f'{day},{mean + seasonal + rng.gauss(0, 2.5):.1f}')
        path = Path('data/raw', site, f'{year}.csv')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('\n'.join(lines) + '\n')

print(f'wrote {len(SITES) * len(YEARS)} files under data/raw/')
