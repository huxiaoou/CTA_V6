"""
ventropy: entropy of volume
"""

import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpVENTROPY(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="VENTROPY", **kwargs)


class CFactorVENTROPY(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpVENTROPY, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpVENTROPY):
            raise TypeError("factor_grp must be CCfgFactorGrpVENTROPY")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    @staticmethod
    def cal_ventropy(tday_minb_data: pd.DataFrame, vol: str = "vol", amount: str = "amount") -> float:
        res = {}
        for k in [vol, amount]:
            v = tday_minb_data[k].astype(float)
            v_pos = v[v > 0]
            if v_pos.empty:
                res[k] = np.nan
            else:
                prob = v_pos / v_pos.sum()
                res[k] = -prob @ np.log(prob) * 100
        # return res[amount] - res[vol]
        # return res[vol]
        return res[amount]

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        maj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "return_c_major"],
        )
        maj_data = maj_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        ventropy = minb_data.groupby(by="trade_date").apply(self.cal_ventropy)
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            maj_data[name_vanilla] = ventropy.rolling(win).sum()
        maj_data = maj_data.reset_index()
        self.rename_ticker(maj_data)
        factor_data = self.get_factor_data(maj_data, bgn_date=bgn_date)
        return factor_data
