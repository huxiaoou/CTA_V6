import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpSIZE(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="SIZE", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorSIZE(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpSIZE, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpSIZE):
            raise TypeError("factor_grp must be CCfgFactorGrpSIZE")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        size_id = "oi_major"
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "return_c_major", size_id],
        )
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            size_ma = major_data[size_id].rolling(window=win, min_periods=int(win * 0.3)).mean()
            major_data[name_vanilla] = -(major_data[size_id] / size_ma - 1)
        n0, n1 = self.cfg.name_vanilla(240), self.cfg.name_vanilla(60)
        major_data[self.cfg.name_diff()] = major_data[n0] - major_data[n1]
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date)
        return factor_data
