"""
reoc: return of efficient openinterest change
"""

import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg, robust_div


class CCfgFactorGrpREOC(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="REOC", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_vol + self.names_diff


class CFactorREOC(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpREOC, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpREOC):
            raise TypeError("factor_grp must be CCfgFactorGrpREOC")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    @staticmethod
    def cal_reoc(trade_day_data: pd.DataFrame, eff: str = "eff", ret: str = "simple") -> float:
        net_data = trade_day_data.iloc[1:, :]
        eff_sum = net_data[eff].sum()
        if eff_sum > 0:
            wgt = net_data[eff] / eff_sum
            reoc = net_data[ret].fillna(0) @ wgt
            return reoc
        else:
            return 0.0

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        maj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "closeI", "oi_major", "vol_major"],
        )
        maj_data = maj_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["simple"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"], scale=1e4)
        minb_data["doi"] = minb_data["oi"].diff().abs()
        minb_data["eff"] = robust_div(minb_data["doi"], minb_data["vol"], nan_val=0)
        reoc = minb_data.groupby(by="trade_date").apply(self.cal_reoc)
        for win, name_vanilla, name_vol in zip(self.cfg.args.wins, self.cfg.names_vanilla, self.cfg.names_vol):
            maj_data[name_vanilla] = reoc.rolling(win).sum()
            maj_data[name_vol] = reoc.rolling(win).std()
        w0, w1 = 240, 5
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_vanilla(w1)
        maj_data[self.cfg.name_diff()] = maj_data[n0] * np.sqrt(w1 / w0) - maj_data[n1]
        maj_data = maj_data.reset_index()
        self.rename_ticker(maj_data)
        factor_data = self.get_factor_data(maj_data, bgn_date=bgn_date)
        return factor_data
