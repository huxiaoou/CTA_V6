"""
ikurt: intra-day kurtosis
"""

import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpIKURT(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="IKURT", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorIKURT(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpIKURT, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpIKURT):
            raise TypeError("factor_grp must be CCfgFactorGrpIKURT")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        maj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major"],
        )
        maj_data = maj_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["simple"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"], scale=1e4)
        ikurt = minb_data.groupby(by="trade_date")["simple"].apply(lambda z: z.kurt())
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            maj_data[name_vanilla] = -ikurt.rolling(win).sum()
        w0, w1 = 240, 20
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_vanilla(w1)
        maj_data[self.cfg.name_diff()] = maj_data[n0] * np.sqrt(w1 / w0) - maj_data[n1]
        maj_data = maj_data.reset_index()
        self.rename_ticker(maj_data)
        factor_data = self.get_factor_data(maj_data, bgn_date=bgn_date)
        return factor_data
