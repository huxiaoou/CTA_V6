import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWinLbd
from solutions.factor import CFactorCORR
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpCVP(CCfgFactorGrpWinLbd):
    def __init__(self, **kwargs):
        super().__init__(factor_class="CVP", **kwargs)


class CFactorCVP(CFactorCORR):
    def __init__(self, factor_grp: CCfgFactorGrpCVP, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpCVP):
            raise TypeError("factor_grp must be CCfgFactorGrpCVP")
        super().__init__(factor_grp=factor_grp, **kwargs)

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        adj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "closeI", "oi_major", "vol_major"],
        )
        adj_data = adj_data.set_index("trade_date")
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["simple"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"], scale=1e4)
        adj_data["vol"] = minb_data.groupby(by="trade_date")["simple"].apply(lambda z: z.std())
        x, y, sort_var = "vol", "closeI", "vol_major"
        self.cal_core(raw_data=adj_data, bgn_date=bgn_date, stp_date=stp_date, x=x, y=y, sort_var=sort_var)
        adj_data = adj_data.reset_index()
        self.rename_ticker(adj_data)
        factor_data = self.get_factor_data(adj_data, bgn_date=bgn_date)
        return factor_data
