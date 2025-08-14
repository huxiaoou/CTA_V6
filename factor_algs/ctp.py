import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWinLbd
from solutions.factor import CFactorCORR


class CCfgFactorGrpCTP(CCfgFactorGrpWinLbd):
    def __init__(self, **kwargs):
        super().__init__(factor_class="CTP", **kwargs)


class CFactorCTP(CFactorCORR):
    def __init__(self, factor_grp: CCfgFactorGrpCTP, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpCTP):
            raise TypeError("factor_grp must be CCfgFactorGrpCTP")
        super().__init__(factor_grp=factor_grp, **kwargs)

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        adj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "closeI", "oi_major", "vol_major"],
        )
        adj_data = adj_data.set_index("trade_date")
        adj_data["aver_oi"] = adj_data["oi_major"].rolling(window=2).mean()
        adj_data["turnover"] = adj_data["vol_major"] / adj_data["aver_oi"]
        x, y, sort_var = "turnover", "closeI", "vol_major"
        self.cal_core(raw_data=adj_data, bgn_date=bgn_date, stp_date=stp_date, x=x, y=y, sort_var=sort_var)
        adj_data = adj_data.reset_index()
        self.rename_ticker(adj_data)
        factor_data = self.get_factor_data(adj_data, bgn_date=bgn_date)
        return factor_data
