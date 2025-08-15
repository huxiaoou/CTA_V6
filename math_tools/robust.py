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
