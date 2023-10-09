import sys
import os
import json
import pandas as pd
import numpy as np
from joblib import Parallel, delayed

current_dir = os.path.dirname(__file__)
sys.path.append(os.path.join(current_dir, '../../../'))

from src.simulation import Simulation

dataset = pd.read_csv(os.path.join(current_dir, '../../../data/b3_returns_v4.csv'))

def run_experiment(start_date):
    print('Start', start_date)
    output_dir = os.path.join(current_dir, 'outputs', start_date)
    os.makedirs(output_dir, exist_ok=True)


    params = dict(
        start_date=start_date,
        initial_amount=5000,
        window_size=252,
        rebalancing_interval=1,
        relocation_interval=None,
    )

    simulation = Simulation(
        dataset=dataset,
        **params,
    )
    initial_target_allocation = np.array(
        [1 if ticker == 'BOVA11' else 0 for ticker in simulation.tickers]
    )
    simulation.initial_target_allocation = initial_target_allocation
    simulation.run()

    history = pd.DataFrame(simulation.history)
    tickers = simulation.tickers

    with open(os.path.join(output_dir, 'tickers.json'), 'w', encoding='utf8') as f:
        json.dump(tickers.tolist(), f)

    with open(os.path.join(output_dir, 'initial_target_allocation.json'), 'w', encoding='utf8') as f:
        json.dump(initial_target_allocation.tolist(), f)

    with open(os.path.join(output_dir, 'parameters.json'), 'w', encoding='utf8') as f:
        json.dump(params, f)

    history.to_csv(os.path.join(output_dir, 'history.csv'), index=False)
    print('End', start_date)

dates = dataset\
    .drop_duplicates("date")\
    .set_index("date")\
    .loc["2018-01-11":"2021-12-31"]\
    .index\
    .values[::7]
print('Starting', len(dates), 'simulations')

Parallel(n_jobs=-1)(delayed(run_experiment)(date) for date in dates)

print('DONE')
