import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames, TFactorName
from solutions.factor import CFactorsByInstru
from math_tools.rolling import cal_rolling_beta


class CCfgFactorGrpBASIS(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="BASIS", **kwargs)

    def name_diff2(self) -> TFactorName:
        return TFactorName(f"{self.factor_class}DIFF2")

    @property
    def names_diff(self) -> TFactorNames:
        return [self.name_diff(), self.name_diff2()]

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_res + self.names_diff


class CFactorBASIS(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpBASIS, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpBASIS):
            raise TypeError("factor_grp must be CCfgFactorGrpBASIS")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        x, y = "basis_rate", "return_c_major"
        adj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", x, y],
        )
        for win, name_vanilla, name_res in zip(self.cfg.args.wins, self.cfg.names_vanilla, self.cfg.names_res):
            adj_data[name_vanilla] = adj_data[x].rolling(window=win, min_periods=int(2 * win / 3)).mean()
            beta = cal_rolling_beta(df=adj_data, x=x, y=y, rolling_window=win)
            adj_data[name_res] = adj_data[y] - adj_data[x] * beta

        w0, w1 = self.cfg.args.wins
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_vanilla(w1)
        adj_data[self.cfg.name_diff()] = adj_data[n0] * np.sqrt(w0 / w1) - adj_data[n1]

        w0, w1 = self.cfg.args.wins
        n0, n1 = self.cfg.name_res(w0), self.cfg.name_res(w1)
        adj_data[self.cfg.name_diff2()] = adj_data[n0] * np.sqrt(w0 / w1) - adj_data[n1]

        self.rename_ticker(adj_data)
        factor_data = self.get_factor_data(adj_data, bgn_date)
        return factor_data
