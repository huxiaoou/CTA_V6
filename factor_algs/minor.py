import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpMINOR(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="MINOR", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_res + self.names_diff


class CFactorMINOR(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpMINOR, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpMINOR):
            raise TypeError("factor_grp must be CCfgFactorGrpMINOR")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        adj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "return_c_major", "return_c_minor"],
        )
        minor, major = "return_c_minor", "return_c_major"
        for win, name_vanilla, name_res in zip(self.cfg.args.wins, self.cfg.names_vanilla, self.cfg.names_res):
            minor_avg = adj_data[minor].rolling(window=win, min_periods=int(2 * win / 3)).mean()
            major_avg = adj_data[major].rolling(window=win, min_periods=int(2 * win / 3)).mean()
            adj_data[name_vanilla] = minor_avg
            adj_data[name_res] = major_avg - minor_avg
        w0, w1 = self.cfg.args.wins
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_vanilla(w1)
        adj_data[self.cfg.name_diff()] = adj_data[n0] * np.power(w0 / w1, 0.5) - adj_data[n1]
        self.rename_ticker(adj_data)
        factor_data = self.get_factor_data(adj_data, bgn_date)
        return factor_data
