import sys
import os
import json
import pandas as pd
current_dir = os.path.dirname(__file__)
sys.path.append(os.path.join(current_dir, '../../'))

from src.simulation import Simulation

output_dir = os.path.join(current_dir, 'outputs')
os.makedirs(output_dir, exist_ok=True)

dataset = pd.read_csv(os.path.join(current_dir, '../../data/b3_returns_v4.csv'))

params = dict(
    start_date='2018-01-11',
    initial_amount=5000,
    window_size=63,
    rebalancing_interval=21,
    relocation_interval=21,
)

simulation = Simulation(
    dataset=dataset,
    **params,
)
simulation.run()

history = pd.DataFrame(simulation.history)
tickers = simulation.tickers

with open(os.path.join(output_dir, 'tickers.json'), 'w', encoding='utf8') as f:
    json.dump(tickers.tolist(), f)

with open(os.path.join(output_dir, 'parameters.json'), 'w', encoding='utf8') as f:
    json.dump(params, f)

history.to_csv(os.path.join(output_dir, 'history.csv'), index=False)
