from pymoo.algorithms.moo.sms import SMSEMOA
from pymoo.optimize import minimize
import pandas as pd
from tqdm import tqdm
import numpy as np
from src.optimization import PortfolioProblem, PortfolioRepair


class Simulation:
    def __init__(
        self,
        dataset,
        start_date,
        initial_amount,
        window_size,
        rebalancing_interval=None,
        relocation_interval=None,
        initial_target_allocation=None,
    ):
        self.dataset = dataset.sort_values('date')
        self.start_date = start_date
        self.initial_amount = initial_amount
        self.window_size = window_size
        self.rebalancing_interval = rebalancing_interval
        self.relocation_interval = relocation_interval
        self.initial_target_allocation = initial_target_allocation
        pivot = pd.pivot_table(
            self.dataset,
            values='return',
            index=['date'],
            columns=['ticker']
        ).sort_index()
        pivot = pivot.assign(date=pd.to_datetime(pivot.index))
        self.pivot = pivot
        self.tickers = pivot.drop(columns='date').columns
        self.history = []

    def optimize_portfolio(self, current_date, prices, amount):
        sliced = self.pivot.loc[:current_date].iloc[-self.window_size-1:-1]
        assert len(sliced) == self.window_size
        mu = sliced.mean(numeric_only=True).values
        cov = sliced.cov(numeric_only=True).values

        problem = PortfolioProblem(mu, cov)
        algorithm = SMSEMOA(repair=PortfolioRepair(prices, amount))
        res = minimize(
            problem,
            algorithm,
            seed=1,
            verbose=False,
        )

        X, F, sharpe = res.opt.get("X", "F", "sharpe")
        F = F * [1, -1]
        max_sharpe = sharpe.argmax()
        return X[max_sharpe]

    def rebalance(self, target_allocation, amount, prices):
        amounts = target_allocation * amount
        new_positions = amounts // prices
        return new_positions

    def run(self):
        all_dates = self.dataset.date.unique().tolist()
        start_date_index = all_dates.index(self.start_date)
        dates = all_dates[start_date_index:]

        amount = self.initial_amount
        target_allocation = self.initial_target_allocation
        allocation = np.array([0] * len(self.tickers))
        unallocated = 1
        positions = np.array([0] * len(self.tickers))
        cash = amount * unallocated

        self.history = [{
            'date': all_dates[start_date_index - 1],
            'amount': amount,
            'target_allocation': target_allocation,
            'allocation': allocation,
            'unallocated': unallocated,
            'positions': positions,
            'cash': cash,
        }]
        for index, current_date in tqdm(list(enumerate(dates))):
            per_ticker = self.dataset[
                self.dataset.date == current_date
            ].set_index('ticker')
            prices = per_ticker.price.loc[self.tickers].values
            amount = prices @ positions + cash

            if self.relocation_interval and index % self.relocation_interval == 0:
                target_allocation = self.optimize_portfolio(
                    current_date,
                    prices,
                    amount
                )

            if self.rebalancing_interval and index % self.rebalancing_interval == 0:
                positions = self.rebalance(target_allocation, amount, prices)

            allocation = prices * positions / amount
            unallocated = 1 - sum(allocation)
            cash = amount * unallocated

            self.history.append({
                'date': current_date,
                'amount': amount,
                'target_allocation': target_allocation,
                'allocation': allocation,
                'unallocated': unallocated,
                'positions': positions,
                'cash': cash,
            })

            positions = np.floor(
                positions / per_ticker.adjust_factor.loc[self.tickers].values
            )
