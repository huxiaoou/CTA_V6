import numpy as np
import pandas as pd
from typing import Literal


def robust_ret_alg(
        x: pd.Series, y: pd.Series, scale: float = 1.0,
        condition: Literal["ne", "ge", "le"] = "ne",
) -> pd.Series:
    """

    :param x: must have the same length as y
    :param y:
    :param scale: return scale
    :param condition:
    :return:
    """
    if condition == "ne":
        return (x / y.where(y != 0, np.nan) - 1) * scale
    elif condition == "ge":
        return (x / y.where(y > 0, np.nan) - 1) * scale
    elif condition == "le":
        return (x / y.where(y < 0, np.nan) - 1) * scale
    else:
        raise ValueError("parameter condition must be 'ne', 'ge', or 'le'.")


def robust_ret_log(x: pd.Series, y: pd.Series, scale: float = 1.0) -> pd.Series:
    """

    :param x: must have the same length as y
    :param y:
    :param scale:
    :return: for log return, x, y are supposed to be positive
    """
    return (np.log(x.where(x > 0, np.nan) / y.where(y > 0, np.nan))) * scale


def robust_div(
        x: pd.Series, y: pd.Series, nan_val: float = np.nan,
        condition: Literal["ne", "ge", "le"] = "ne",
) -> pd.Series:
    """

    :param x: must have the same length as y
    :param y:
    :param nan_val:
    :param condition:
    :return:
    """

    if condition == "ne":
        return (x / y.where(y != 0, np.nan)).fillna(nan_val)
    elif condition == "ge":
        return (x / y.where(y > 0, np.nan)).fillna(nan_val)
    elif condition == "le":
        return (x / y.where(y < 0, np.nan)).fillna(nan_val)
    else:
        raise ValueError("parameter condition must be 'ne', 'ge', or 'le'.")


def gen_exp_wgt(k: int, rate: float = 0.25) -> np.ndarray:
    k0, d = k // 2, k % 2
    rou = np.power(rate, 1 / (k0 - 1)) if k0 > 1 else 1
    sgn = np.array([1] * k0 + [0] * d + [-1] * k0)
    val = np.power(rou, list(range(k0)) + [k0] * d + list(range(k0 - 1, -1, -1)))
    s = sgn * val
    abs_sum = np.abs(s).sum()
    wgt = (s / abs_sum) if abs_sum > 0 else np.zeros(k)
    return wgt


