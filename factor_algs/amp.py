import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWinLbd, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpAMP(CCfgFactorGrpWinLbd):
    def __init__(self, **kwargs):
        super().__init__(factor_class="AMP", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorAMP(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpAMP, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpAMP):
            raise TypeError("factor_grp must be CCfgFactorGrpAMP")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_amp(self, tday_minb_data: pd.DataFrame, sort_var: str = "amp", ret: str = "ret") -> pd.Series:
        sort_data = tday_minb_data.sort_values(by=sort_var, ascending=False)
        res = {}
        for lbd, name_lbd in zip(self.cfg.args.lbds, self.cfg.names_lbd):
            pick_size = int(len(sort_data) * lbd)
            ret_h = sort_data[ret].head(pick_size).mean()
            # ret_l = sort_data[ret].tail(pick_size).mean()
            # res[name_lbd] = (ret_h - ret_l) * 1e4
            res[name_lbd] = ret_h * 1e4
        return pd.Series(res)

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "closeI"],
        )
        major_data = major_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["ret"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"]).fillna(0)
        minb_data["amp"] = robust_ret_alg(minb_data["high"], minb_data["low"], scale=1e4)
        amp_data: pd.DataFrame = minb_data.groupby(by="trade_date").apply(self.cal_amp)
        input_data = pd.merge(
            left=major_data,
            right=amp_data.reset_index(),
            on="trade_date",
            how="left",
        )
        for lbd, name_lbd in zip(self.cfg.args.lbds, self.cfg.names_lbd):
            for win in self.cfg.args.wins:
                name_vanilla = self.cfg.name_vanilla(win, lbd)
                input_data[name_vanilla] = input_data[name_lbd].rolling(window=win).mean()

        w0, w1, lbd = 240, 20, 0.5
        n0, n1 = self.cfg.name_vanilla(win=w0, lbd=lbd), self.cfg.name_vanilla(win=w1, lbd=lbd)
        input_data[self.cfg.name_diff()] = input_data[n0] * np.sqrt(w0 / w1) - input_data[n1]
        self.rename_ticker(input_data)
        factor_data = self.get_factor_data(input_data, bgn_date=bgn_date)
        return factor_data
