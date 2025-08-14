import numpy as np
import pandas as pd


def cal_rolling_corr(df: pd.DataFrame, x: str, y: str, rolling_window: int) -> pd.Series:
    xyb: pd.Series = (df[x] * df[y]).rolling(window=rolling_window).mean()
    xxb: pd.Series = (df[x] * df[x]).rolling(window=rolling_window).mean()
    yyb: pd.Series = (df[y] * df[y]).rolling(window=rolling_window).mean()
    xb: pd.Series = df[x].rolling(window=rolling_window).mean()
    yb: pd.Series = df[y].rolling(window=rolling_window).mean()
    cov_xy: pd.Series = xyb - xb * yb
    cov_xx: pd.Series = xxb - xb * xb
    cov_yy: pd.Series = yyb - yb * yb

    # due to float number precision, cov_xx or cov_yy could be slightly negative
    cov_xx = cov_xx.mask(cov_xx < 1e-10, other=0)
    cov_yy = cov_yy.mask(cov_yy < 1e-10, other=0)

    sqrt_cov_xx_yy: pd.Series = np.sqrt(cov_xx * cov_yy)
    s: pd.Series = cov_xy / sqrt_cov_xx_yy.where(sqrt_cov_xx_yy > 0, np.nan)
    return s


def cal_rolling_beta(df: pd.DataFrame, x: str, y: str, rolling_window: int) -> pd.Series:
    xyb: pd.Series = (df[x] * df[y]).rolling(window=rolling_window).mean()
    xxb: pd.Series = (df[x] * df[x]).rolling(window=rolling_window).mean()
    xb: pd.Series = df[x].rolling(window=rolling_window).mean()
    yb: pd.Series = df[y].rolling(window=rolling_window).mean()
    cov_xy: pd.Series = xyb - xb * yb
    cov_xx: pd.Series = xxb - xb * xb
    s: pd.Series = cov_xy / cov_xx.where(cov_xx > 0, np.nan)
    return s


def cal_rolling_beta_alpha_res(
        df: pd.DataFrame, x: str, y: str, rolling_window: int,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    xyb: pd.Series = (df[x] * df[y]).rolling(window=rolling_window).mean()
    xxb: pd.Series = (df[x] * df[x]).rolling(window=rolling_window).mean()
    xb: pd.Series = df[x].rolling(window=rolling_window).mean()
    yb: pd.Series = df[y].rolling(window=rolling_window).mean()
    cov_xy: pd.Series = xyb - xb * yb
    cov_xx: pd.Series = xxb - xb * xb
    beta: pd.Series = cov_xy / cov_xx.where(cov_xx > 0, np.nan)
    alpha: pd.Series = yb - beta * xb
    res: pd.Series = df[y] - beta * df[x] - alpha
    return beta, alpha, res


def cal_rolling_beta_res(
        df: pd.DataFrame, x: str, y: str, rolling_window: int,
) -> tuple[pd.Series, pd.Series]:
    xyb: pd.Series = (df[x] * df[y]).rolling(window=rolling_window).mean()
    xxb: pd.Series = (df[x] * df[x]).rolling(window=rolling_window).mean()
    cov_xy: pd.Series = xyb
    cov_xx: pd.Series = xxb
    beta: pd.Series = cov_xy / cov_xx.where(cov_xx > 0, np.nan)
    res: pd.Series = df[y] - beta * df[x]
    return beta, res


def cal_top_corr(sub_data: pd.DataFrame, x: str, y: str, sort_var: str, top_size: int, ascending: bool = False):
    sorted_data = sub_data.sort_values(by=sort_var, ascending=ascending)
    top_data = sorted_data.head(top_size)
    r = top_data[[x, y]].corr(method="spearman").at[x, y]
    return r


def cal_rolling_top_corr(
        raw_data: pd.DataFrame,
        bgn_date: str, stp_date: str,
        win: int, top: float,
        x: str, y: str,
        sort_var: str, direction: int,
) -> pd.Series:
    top_size = int(win * top) + 1
    r_data = {}
    for i, trade_date in enumerate(raw_data.index):
        trade_date: str
        if trade_date < bgn_date:
            continue
        elif trade_date >= stp_date:
            break
        sub_data = raw_data.iloc[i - win + 1: i + 1]
        r_data[trade_date] = cal_top_corr(sub_data, x=x, y=y, sort_var=sort_var, top_size=top_size)
    return pd.Series(r_data) * direction
