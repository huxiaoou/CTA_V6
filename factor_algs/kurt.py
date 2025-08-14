import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpKURT(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="KURT", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return super().names_vanilla + self.names_diff


class CFactorKURT(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpKURT, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpKURT):
            raise TypeError("factor_grp must be CCfgFactorGrpKURT")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "return_c_major"],
        )
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            major_data[name_vanilla] = -major_data["return_c_major"].rolling(window=win).kurt()
        w0, w1 = 60, 10
        n0, n1 = self.cfg.name_vanilla(w0), self.cfg.name_vanilla(w1)
        major_data[self.cfg.name_diff()] = major_data[n0] - major_data[n1]
        self.rename_ticker(major_data)
        factor_data = self.get_factor_data(major_data, bgn_date)
        return factor_data
