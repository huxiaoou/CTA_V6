from itertools import product
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWinLbd, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpWSPLIT(CCfgFactorGrpWinLbd):
    def __init__(self, **kwargs):
        super().__init__(factor_class="WSPLIT", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla


class CFactorWSPLIT(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpWSPLIT, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpWSPLIT):
            raise TypeError("factor_grp must be CCfgFactorGrpWSPLIT")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_diff_ret(self, trade_day_data: pd.DataFrame, ret: str = "simple") -> pd.Series:
        res: dict[str, float] = {}
        for lbd, name_lbd in zip(self.cfg.args.lbds, self.cfg.names_lbd):
            k = int(len(trade_day_data) * lbd)
            high_ret = trade_day_data[ret].head(k).sum()
            low_ret = trade_day_data[ret].tail(k).sum()
            res[name_lbd] = high_ret - low_ret
        return pd.Series(res)

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "return_c_major"],
        )
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["simple"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"], scale=1e4)
        minb_data = minb_data.sort_values(by=["trade_date", "amount"], ascending=[True, False])
        lbd_data = minb_data.groupby(by="trade_date").apply(self.cal_diff_ret)
        major_data = major_data.merge(right=lbd_data, how="left", on="trade_date")
        for win, lbd in product(self.cfg.args.wins, self.cfg.args.lbds):
            name_lbd = self.cfg.name_lbd(lbd)
            name_vanilla = self.cfg.name_vanilla(win, lbd)
            major_data[name_vanilla] = major_data[name_lbd].rolling(win).mean()
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date)
        return factor_data
