"""
onr: Overnight Return
"""

import datetime as dt
import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpONR(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="ONR", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorONR(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpONR, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpONR):
            raise TypeError("factor_grp must be CCfgFactorGrpONR")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        maj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "return_c_major"],
        )
        maj_data = maj_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["onr"] = robust_ret_alg(minb_data["open"], minb_data["pre_close"], scale=1e4)
        minb_data["datetime"] = minb_data["timestamp"].map(lambda z: dt.datetime.fromtimestamp(z))
        minb_data["hour"] = minb_data["datetime"].map(lambda z: z.hour)
        minb_data_day = minb_data.query("hour >= 8 and hour <= 15")
        maj_data["onr"] = minb_data_day.groupby(by="trade_date")["onr"].first()
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            maj_data[name_vanilla] = -maj_data["onr"].rolling(window=win).sum()
        w0, w1 = 120, 5
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_vanilla(w1)
        maj_data[self.cfg.name_diff()] = maj_data[n0] * np.sqrt(w1 / w0) - maj_data[n1]
        maj_data = maj_data.reset_index()
        self.rename_ticker(maj_data)
        factor_data = self.get_factor_data(maj_data, bgn_date=bgn_date)
        return factor_data
