from pymoo.core.problem import ElementwiseProblem
from pymoo.core.repair import Repair
import numpy as np


class PortfolioProblem(ElementwiseProblem):

    def __init__(self, mu, cov, **kwargs):
        super().__init__(n_var=len(mu), n_obj=2, xl=0.0, xu=1.0, **kwargs)
        self.mu = np.exp(mu)
        self.cov = cov

    def _evaluate(self, x, out, *args, **kwargs):
        exp_return = np.log(x @ self.mu + (1 - x.sum()))
        exp_risk = np.sqrt(x.T @ self.cov @ x)
        sharpe = exp_return / exp_risk

        out["F"] = [exp_risk, -exp_return]
        out["sharpe"] = sharpe


class PortfolioRepair(Repair):
    def __init__(self, prices, total_investing_amount):
        super().__init__()
        self.prices = prices
        self.total_investing_amount = total_investing_amount

    def _do(self, problem, X, **kwargs):
        amounts = X * self.total_investing_amount
        X[amounts < self.prices] = 0
        X_sum = X.sum(axis=1, keepdims=True)
        X = X / np.where(X_sum != 0, X_sum, 1)
        amounts = X * self.total_investing_amount
        return self.prices * (amounts // self.prices) / self.total_investing_amount
