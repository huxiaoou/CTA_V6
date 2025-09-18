from itertools import product
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames, TFactorName
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpACR(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="ACR", **kwargs)

    @property
    def vars_to_cal(self) -> list[str]:
        return ["simple", "vol"]

    def name_acr(self, var_to_cal: str) -> TFactorName:
        return TFactorName(f"{self.factor_class}{var_to_cal}")

    def name_x(self, win: int, var_to_cal: str) -> TFactorName:
        return TFactorName(f"{self.factor_class}{win:03d}{var_to_cal}")

    @property
    def factor_names(self) -> TFactorNames:
        return [
            self.name_x(win, var_to_cal)
            for var_to_cal, win in product(self.vars_to_cal, self.args.wins)
        ] + self.names_diff


class CFactorACR(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpACR, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpACR):
            raise TypeError("factor_grp must be CCfgFactorGrpACR")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_acr(self, tday_minb_data: pd.DataFrame) -> pd.Series:
        res: dict[str, float] = {}
        for var_to_cal in self.cfg.vars_to_cal:
            name_acr = self.cfg.name_acr(var_to_cal)
            s0 = tday_minb_data[var_to_cal].fillna(0)
            if (s0.iloc[1:].std() > 0) and (s0.iloc[:-1].std() > 0):
                res[name_acr] = -s0.autocorr(lag=1)
            else:
                res[name_acr] = 0.0
        return pd.Series(res)

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "closeI"],
        )
        major_data = major_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["simple"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"], scale=1e4)
        acr_data: pd.DataFrame = minb_data.groupby(by="trade_date").apply(self.cal_acr)
        input_data = pd.merge(
            left=major_data,
            right=acr_data.reset_index(),
            on="trade_date",
            how="left",
        )
        for win, var_to_cal in product(self.cfg.args.wins, self.cfg.vars_to_cal):
            name_acr, name_x = self.cfg.name_acr(var_to_cal), self.cfg.name_x(win, var_to_cal)
            input_data[name_x] = input_data[name_acr].rolling(window=win).mean()
        w0, w1 = 120, 8
        v0, v1 = self.cfg.vars_to_cal
        n0, n1 = self.cfg.name_x(w0, v0), self.cfg.name_x(w1, v1)
        input_data[self.cfg.name_diff()] = input_data[n1] - input_data[n0]
        self.rename_ticker(input_data)
        factor_data = self.get_factor_data(input_data, bgn_date=bgn_date)
        return factor_data
