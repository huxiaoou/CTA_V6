"""
CNVG: Volatility / Ma * Sign
"""

import numpy as np
import pandas as pd
from itertools import product
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames, TFactorName
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpCNVG(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="CNVG", **kwargs)

    @property
    def vars_to_cal(self) -> list[str]:
        return ["cls", "vol", "amt"]

    def name_ma(self, win: int, var_to_cal: str) -> TFactorName:
        return TFactorName(f"{self.factor_class}{win:03d}{var_to_cal}")

    def names_ma(self, var_to_cal: str) -> TFactorNames:
        return [self.name_ma(win=win, var_to_cal=var_to_cal) for win in self.args.wins]

    def name_f(self, var_to_cal: str) -> TFactorName:
        return TFactorName(f"{self.factor_class}{var_to_cal}")

    @property
    def factor_names(self) -> TFactorNames:
        return [self.name_f(var_to_cal) for var_to_cal in self.vars_to_cal]


class CFactorCNVG(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpCNVG, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpCNVG):
            raise TypeError("factor_grp must be CCfgFactorGrpCNVG")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp
        self.sgn_win = min(120, max(factor_grp.args.wins))
        self.ben_win = min(120, max(factor_grp.args.wins))

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        cls, vol, amt = "close_major", "vol_major", "amount_major"
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major"] + [cls, vol, amt],
        )
        major_data = major_data.rename(columns={cls: "cls", vol: "vol", amt: "amt"})
        for win, var_to_cal in product(self.cfg.args.wins, self.cfg.vars_to_cal):
            major_data[self.cfg.name_ma(win, var_to_cal)] = major_data[var_to_cal].rolling(window=win).mean()
        for var_to_cal in self.cfg.vars_to_cal:
            name_f, names_ma = self.cfg.name_f(var_to_cal), self.cfg.names_ma(var_to_cal)
            sgn = -np.sign(major_data[var_to_cal] / major_data[var_to_cal].rolling(window=self.sgn_win).mean() - 1)
            name_benchmark = self.cfg.name_ma(self.ben_win, var_to_cal)
            major_data[name_f] = (major_data[names_ma].std(axis=1) / major_data[name_benchmark]) * sgn
            # major_data[name_f] = major_data[names_ma].std(axis=1) * sgn * (-1.0)
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date=bgn_date)
        return factor_data
