import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpIDR(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="IDR", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorIDR(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpIDR, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpIDR):
            raise TypeError("factor_grp must be CCfgFactorGrpIDR")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "open_major", "close_major"],
        )
        major_data["idr"] = robust_ret_alg(
            x=major_data["close_major"],
            y=major_data["open_major"],
        )
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            major_data[name_vanilla] = major_data["idr"].rolling(window=win).sum()
        w0, w1 = 240, 5
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_vanilla(w1)
        major_data[self.cfg.name_diff()] = major_data[n0] * np.sqrt(w1 / w0) - major_data[n1]
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date)
        return factor_data
