import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpVAL(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="VAL", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_pa + self.names_diff


class CFactorVAL(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpVAL, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpVAL):
            raise TypeError("factor_grp must be CCfgFactorGrpVAL")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp
        self.val_var = "close_major"

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", self.val_var],
        )
        for win, name_vanilla, name_pa in zip(self.cfg.args.wins, self.cfg.names_vanilla, self.cfg.names_pa):
            major_data[name_vanilla] = robust_ret_alg(
                x=major_data[self.val_var],
                y=major_data[self.val_var].shift(win),
                scale=100,
            )
            major_data[name_pa] = robust_ret_alg(
                x=major_data[self.val_var],
                y=major_data[self.val_var].rolling(win).mean(),
                scale=100,
            )
        w0, w1 = 240, 60
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_vanilla(w1)
        major_data[self.cfg.name_diff()] = major_data[n0] * np.sqrt(w1 / w0) - major_data[n1]
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date)
        return factor_data
