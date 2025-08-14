import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg, robust_ret_log


class CCfgFactorGrpJUMP(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="JUMP", **kwargs)


class CFactorJUMP(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpJUMP, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpJUMP):
            raise TypeError("factor_grp must be CCfgFactorGrpJUMP")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    @staticmethod
    def cal_jump(tday_minb_data: pd.DataFrame, simple: str = "simple", compound: str = "compound") -> float:
        net_data = tday_minb_data.iloc[2:-2, :]
        if net_data.empty:
            return np.nan
        d = net_data[simple] - net_data[compound]
        residual = 2 * d - net_data[compound] ** 2
        return residual.mean()  # type:ignore

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "vol_major", "return_c_major"],
        )
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["simple"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"], scale=1e4)
        minb_data["compound"] = robust_ret_log(minb_data["close"], minb_data["pre_close"], scale=1e4)
        jump_data = minb_data.groupby(by="trade_date").apply(self.cal_jump)
        input_data = pd.merge(
            left=major_data,
            right=jump_data.reset_index().rename(columns={0: "jump"}),
            on="trade_date",
            how="left",
        )
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            input_data[name_vanilla] = input_data["jump"].rolling(window=win).mean()
        self.rename_ticker(input_data)
        factor_data = self.get_factor_data(input_data, bgn_date=bgn_date)
        return factor_data
