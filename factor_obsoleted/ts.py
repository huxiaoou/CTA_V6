import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.rolling import cal_rolling_beta


class CCfgFactorGrpTS(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="TS", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_res + self.names_diff


class CFactorTS(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpTS, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpTS):
            raise TypeError("factor_grp must be CCfgFactorGrpTS")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    @staticmethod
    def cal_roll_return(x: pd.Series, ticker_n: str, ticker_d: str, prc_n: str, prc_d: str):
        if x[ticker_n] == "" or x[ticker_d] == "":
            return np.nan
        if x[prc_d] > 0:
            cntrct_d, cntrct_n = x[ticker_d].split(".")[0], x[ticker_n].split(".")[0]
            month_d, month_n = int(cntrct_d[-2:]), int(cntrct_n[-2:])
            dlt_month = (month_d - month_n) % 12
            return (x[prc_n] / x[prc_d] - 1) / dlt_month * 12 * 100 if dlt_month > 0 else np.nan
        else:
            return np.nan

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        adj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "ticker_minor", "close_major", "close_minor", "return_c_major"],
        )
        adj_data[["ticker_major", "ticker_minor"]] = adj_data[["ticker_major", "ticker_minor"]].fillna("")
        adj_data["ts"] = adj_data.apply(
            self.cal_roll_return,
            args=("ticker_major", "ticker_minor", "close_major", "close_minor"),
            axis=1,
        )
        x, y = "ts", "return_c_major"
        for win, name_vanilla, name_res in zip(self.cfg.args.wins, self.cfg.names_vanilla, self.cfg.names_res):
            adj_data[name_vanilla] = adj_data["ts"].rolling(window=win, min_periods=int(2 * win / 3)).mean()
            beta = cal_rolling_beta(df=adj_data, x=x, y=y, rolling_window=win)
            adj_data[name_res] = -(adj_data[y] - adj_data[x] * beta)
        n0, n1 = self.cfg.name_vanilla(120), self.cfg.name_res(60)
        adj_data[self.cfg.name_diff()] = adj_data[n0] + 500 * adj_data[n1]
        self.rename_ticker(adj_data)
        factor_data = self.get_factor_data(adj_data, bgn_date)
        return factor_data
