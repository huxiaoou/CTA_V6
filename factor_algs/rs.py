import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpRS(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="RS", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_pa + self.names_la + self.names_diff


class CFactorRS(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpRS, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpRS):
            raise TypeError("factor_grp must be CCfgFactorGrpRS")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp
        self.__win_min = min(5, min(self.cfg.args.wins))

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        adj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "stock"],
        )
        adj_data["stock"] = adj_data["stock"].ffill(limit=self.__win_min).fillna(0)
        for win, rspa, rsla in zip(self.cfg.args.wins, self.cfg.names_pa, self.cfg.names_la):
            ma = adj_data["stock"].rolling(window=win).mean()
            s = adj_data["stock"] / ma.where(ma > 0, np.nan)
            adj_data[rspa] = 1 - s
            la = adj_data["stock"].shift(win)
            s = adj_data["stock"] / la.where(la > 0, np.nan)
            adj_data[rsla] = 1 - s
        w0, w1 = self.cfg.args.wins
        n0, n1 = self.cfg.name_pa(w0), self.cfg.name_pa(w1)
        adj_data[self.cfg.name_diff()] = adj_data[n0] - adj_data[n1]
        self.rename_ticker(adj_data)
        factor_data = self.get_factor_data(adj_data, bgn_date)
        return factor_data
