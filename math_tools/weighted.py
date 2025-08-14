import numpy as np
import pandas as pd
from typing import Union


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
