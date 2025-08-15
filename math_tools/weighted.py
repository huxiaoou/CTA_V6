import numpy as np
import pandas as pd
from typing import Union


def gen_exp_wgt(k: int, rate: float = 0.25) -> np.ndarray:
    k0, d = k // 2, k % 2
    rou = np.power(rate, 1 / (k0 - 1)) if k0 > 1 else 1
    sgn = np.array([1] * k0 + [0] * d + [-1] * k0)
    val = np.power(rou, list(range(k0)) + [k0] * d + list(range(k0 - 1, -1, -1)))
    s = sgn * val
    abs_sum = np.abs(s).sum()
    wgt = (s / abs_sum) if abs_sum > 0 else np.zeros(k)
    return wgt


def auto_weight_sum(x: pd.Series) -> float:
    weight = x.abs() / x.abs().sum()
    return x @ weight


def weighted_volatility(x: pd.Series, wgt: pd.Series = None) -> float:
    if wgt is None:
        return x.std()
    else:
        mu = x @ wgt
        x2 = (x ** 2) @ wgt
        return np.sqrt(x2 - mu ** 2)


def wcov(x: Union[pd.Series, np.ndarray], y: Union[pd.Series, np.ndarray], w: Union[pd.Series, np.ndarray]) -> float:
    """

    :param x:
    :param y:
    :param w: must have w.sum() = 1, all(w >= 0) = True
    :return:
    """
    return w @ (x * y) - (w @ x) * (w @ y)


def wic(x: Union[pd.Series, np.ndarray], y: Union[pd.Series, np.ndarray], w: Union[pd.Series, np.ndarray]) -> float:
    w_norm = w / np.abs(w).sum()
    vxy = wcov(x, y, w_norm)
    vxx = wcov(x, x, w_norm)
    vyy = wcov(y, y, w_norm)
    if vxx > 0 and vyy > 0:
        return vxy / np.sqrt(vxx * vyy)
    else:
        print(f"{vxx=:.8f}, {vyy=:.8f}")
        return 0
