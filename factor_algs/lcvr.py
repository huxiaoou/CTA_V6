"""
lcvr: lagging correlation of volume and (absolute) ret
"""

import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpLCVR(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="LCVR", **kwargs)


class CFactorLCVR(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpLCVR, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpLCVR):
            raise TypeError("factor_grp must be CCfgFactorGrpLCVR")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    @staticmethod
    def cal_lcvr(tday_minb_data: pd.DataFrame, vol: str = "vol", ret: str = "ret") -> float:
        size = len(tday_minb_data)
        sv = tday_minb_data[vol].fillna(0).head(size - 1)
        sr = tday_minb_data[ret].shift(-1).fillna(0).head(size - 1)
        sa = tday_minb_data[ret].abs().shift(-1).fillna(0).head(size - 1)
        rvr, rva = 0, 0
        if sv.std() > 0:
            if sr.std() > 0:
                rvr = -sv.corr(other=sr)
            if sa.std() > 0:
                rva = -sv.corr(other=sa)
        return rvr + rva

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "closeI"],
        )
        major_data = major_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["ret"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"]).fillna(0)
        lcvr_data: pd.Series = minb_data.groupby(by="trade_date").apply(self.cal_lcvr)
        input_data = pd.merge(
            left=major_data,
            right=lcvr_data.reset_index().rename(columns={0: "lcvr"}),
            on="trade_date",
            how="left",
        )
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            input_data[name_vanilla] = input_data["lcvr"].rolling(window=win).mean()
        self.rename_ticker(input_data)
        factor_data = self.get_factor_data(input_data, bgn_date=bgn_date)
        return factor_data
