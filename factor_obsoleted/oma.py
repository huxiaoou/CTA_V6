"""
oma: Order of Moving Average
"""

import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames, TFactorName
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpOMA(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="OMA", **kwargs)

    def name_vol(self, win: int) -> TFactorName:
        return TFactorName(f"{self.factor_class}{win:03d}V")

    @property
    def names_vol(self) -> TFactorNames:
        return [self.name_vol(win) for win in self.args.wins]

    @property
    def name_order_p(self) -> TFactorName:
        return TFactorName(f"{self.factor_class}P")

    @property
    def name_order_v(self) -> TFactorName:
        return TFactorName(f"{self.factor_class}V")

    @property
    def name_order_s(self) -> TFactorName:
        return TFactorName(f"{self.factor_class}S")

    @property
    def factor_names(self) -> TFactorNames:
        return [self.name_order_p, self.name_order_v, self.name_order_s]


class CFactorOMA(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpOMA, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpOMA):
            raise TypeError("factor_grp must be CCfgFactorGrpOMA")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp
        self.prc, self.vol = "closeI", "vol_major"

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", self.prc, self.vol],
        )
        for win, name_vanilla, name_vol in zip(self.cfg.args.wins, self.cfg.names_vanilla, self.cfg.names_vol):
            major_data[name_vanilla] = major_data[self.prc].rolling(window=win).mean()
            major_data[name_vol] = major_data[self.vol].rolling(window=win).mean()
        rnk_p = pd.Series({k: v for v, k in enumerate(self.cfg.names_vanilla)})
        rnk_v = pd.Series({k: v for v, k in enumerate(self.cfg.names_vol)})
        major_data[self.cfg.name_order_p] = -major_data[self.cfg.names_vanilla].corrwith(
            other=rnk_p, axis=1, method="spearman",
        ).fillna(0)
        major_data[self.cfg.name_order_v] = -major_data[self.cfg.names_vol].corrwith(
            other=rnk_v, axis=1, method="spearman",
        ).fillna(0)
        major_data[self.cfg.name_order_s] = major_data[self.cfg.name_order_p] + major_data[self.cfg.name_order_v]
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date)
        return factor_data
