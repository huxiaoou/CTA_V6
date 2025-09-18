import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpSKEW(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="SKEW", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return super().names_vanilla + self.names_delay + self.names_diff


class CFactorSKEW(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpSKEW, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpSKEW):
            raise TypeError("factor_grp must be CCfgFactorGrpSKEW")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "return_c_major"],
        )
        for win, name_vanilla, name_delay in zip(self.cfg.args.wins, self.cfg.names_vanilla, self.cfg.names_delay):
            major_data[name_vanilla] = -major_data["return_c_major"].rolling(window=win).skew()
            major_data[name_delay] = major_data[name_vanilla].shift(1)
        w0, w1 = 120, 10
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_delay(w1)
        major_data[self.cfg.name_diff()] = major_data[n0] + major_data[n1]
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date)
        return factor_data
