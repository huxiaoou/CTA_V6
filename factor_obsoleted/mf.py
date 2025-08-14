import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWin, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpMF(CCfgFactorGrpWin):
    def __init__(self, **kwargs):
        super().__init__(factor_class="MF", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorMF(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpMF, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpMF):
            raise TypeError("factor_grp must be CCfgFactorGrpMF")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    @staticmethod
    def cal_mf(tday_minb_data: pd.DataFrame, money: str = "amount", ret: str = "freq_ret") -> float:
        wgt = tday_minb_data[money] / tday_minb_data[money].sum()
        sgn = tday_minb_data[ret].fillna(0)
        mf = -wgt @ sgn
        return mf

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)
        major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "vol_major", "return_c_major"],
        )
        minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        minb_data["freq_ret"] = robust_ret_alg(minb_data["close"], minb_data["pre_close"], scale=1e4)
        mf_data = minb_data.groupby(by="trade_date").apply(self.cal_mf)
        input_data = pd.merge(
            left=major_data,
            right=mf_data.reset_index().rename(columns={0: "mf"}),
            on="trade_date",
            how="left",
        )
        for win, name_vanilla in zip(self.cfg.args.wins, self.cfg.names_vanilla):
            input_data[name_vanilla] = input_data["mf"].rolling(window=win).mean()
        n0, n1 = self.cfg.name_vanilla(1), self.cfg.name_vanilla(5)
        input_data[self.cfg.name_diff()] = input_data[n0] - input_data[n1]
        self.rename_ticker(input_data)
        factor_data = self.get_factor_data(input_data, bgn_date=bgn_date)
        return factor_data
