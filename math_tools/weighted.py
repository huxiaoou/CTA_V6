import numpy as np
import pandas as pd
from typing import Union


def adjust_weights(raw_weights: pd.DataFrame, tot_wgt: pd.DataFrame, weight: str = "weight") -> pd.DataFrame:
    """

    :param raw_weights: a pd.DataFrame with columns at least: ["trade_date", "instrument", "weight"]
    :param tot_wgt: a pd.DataFrame with columns at least: ["trade_date", "tot_wgt"]
    :param weight: a str representing the raw weight to be adjusted
    :return:
    """
    new_data = pd.merge(
        left=raw_weights,
        right=tot_wgt[["trade_date", "tot_wgt"]],
        on="trade_date",
        how="left",
    )
    new_data[weight] = new_data[weight] * new_data["tot_wgt"]
    weights = new_data[["trade_date", "instrument", weight]]
    return weights


def map_to_weight(data: Union[pd.DataFrame, pd.Series], rate: float = 0.25) -> Union[pd.DataFrame, pd.Series]:
    k = len(data)
    k0, d, r0 = k // 2, k % 2, (k + 1) / 2
    rou = np.power(rate, 1 / (k0 - 1)) if k0 > 1 else 1
    data_filled = data.fillna(data.median())
    data_rnk = data_filled.rank() - r0
    val, sgn = np.power(rou, r0 - data_rnk.abs()), np.sign(data_rnk)
    raw_wgt = sgn * val
    wgt = raw_wgt / raw_wgt.abs().sum()
    return wgt


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
        w = wgt / wgt.abs().sum()
        mu = x @ w
        x2 = (x ** 2) @ w
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


def dispersion(x: Union[pd.Series, np.ndarray]) -> float:
    return x @ x - len(x) * x.mean() ** 2


def decompose_dispersion(data: pd.DataFrame, ret: str = "ret", sector: str = "sector") -> tuple[float, float, float]:
    n, m = len(data), data[ret].mean()
    dp_tot = dispersion(data[ret])
    res = []
    for sec, sec_ret in data.groupby(by=sector)[ret]:
        res.append({
            "sector": sec,
            "within": dispersion(sec_ret),
            "between": len(sec_ret) * (sec_ret.mean() - m) ** 2
        })
    res = pd.DataFrame(res)
    dp_within, dp_between = res["within"].sum(), res["between"].sum()
    if abs(dp_tot - dp_within - dp_between) > 1e-6:
        raise ValueError(f"total = {dp_tot:.4f}, within = {dp_within:.4f}, between = {dp_between:.4f}")
    return dp_tot, dp_within, dp_between


if __name__ == "__main__":
    test_data = pd.DataFrame({
        "ret": [1, 2, 3, 4, 5, 6, 7, 8],
        "sector": list("abbcccdd"),
    })
    tot, within, between = decompose_dispersion(test_data)
    print(f"tot     = {tot:>8.4f}")
    print(f"within  = {within:>8.4f}")
    print(f"between = {between:>8.4f}")
