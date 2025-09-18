import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpIDV(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="IDV", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorIDV(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpIDV, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpIDV):
            raise TypeError("factor_grp must be CCfgFactorGrpIDV")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "return_c_major"],
        )
        major_data = major_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["simple"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"], scale=1e4)
        major_data["vol"] = minb_data.groupby(by="trade_date")["simple"].apply(lambda z: z.std())
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            mu = major_data["vol"].rolling(window=win).mean()
            sd = major_data["vol"].rolling(window=win).std()
            major_data[name_vanilla] = -((major_data["vol"] - mu) / sd.where(sd > 0, np.nan)).fillna(0)
        n0, n1 = self.cfg.name_vanilla(60), self.cfg.name_vanilla(10)
        major_data[self.cfg.name_diff()] = major_data[n0] - major_data[n1]
        major_data = major_data.reset_index()
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date=bgn_date)
        return factor_data
