"""
TR: Turnover Rate
"""
import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_div


class CCfgFactorGrpTR(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="TR", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorTR(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpTR, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpTR):
            raise TypeError("factor_grp must be CCfgFactorGrpTR")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        adj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "oi_major", "vol_major", "return_c_major"],
        )
        adj_data["aver_oi"] = adj_data["oi_major"].rolling(window=2).mean()
        adj_data["turnover"] = robust_div(x=adj_data["vol_major"], y=adj_data["aver_oi"], nan_val=1.0)
        adj_data["ret_adj"] = (adj_data["return_c_major"] * adj_data["turnover"]).fillna(0)

        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            adj_data[name_vanilla] = adj_data["ret_adj"].rolling(window=win, min_periods=int(2 * win / 3)).sum()

        wa, wb = 240, 60
        na, nb = self.cfg.name_vanilla(wa), self.cfg.name_vanilla(wb)
        adj_data[self.cfg.name_diff()] = adj_data[na] * np.sqrt(wb / wa) - adj_data[nb]
        self.rename_ticker(adj_data)
        factor_data = self.get_factor_data(adj_data, bgn_date=bgn_date)
        return factor_data
