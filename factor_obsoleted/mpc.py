"""
mpc: Member Position Change
"""

import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWinLbd
from solutions.factor import CFactorsByInstru

pd.set_option("display.unicode.east_asian_width", True)


class CCfgFactorGrpMPC(CCfgFactorGrpWinLbd):
    def __init__(self, **kwargs):
        super().__init__(factor_class="MPC", **kwargs)

    @staticmethod
    def name_top(top: int) -> str:
        return f"TOP{top:02d}"


class CFactorMPC(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpMPC, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpMPC):
            raise TypeError("factor_grp must be CCfgFactorGrpMPC")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp
        self.TOT_SIZE = 20

    def cal_top(self, trade_day_data: pd.DataFrame, x: str) -> pd.Series:
        """

        :param trade_day_data:
        :param x: variable to calculate, options =("long_hld", "long_chg", "short_hld", "short_chg")
        :return:
        """
        res = {}
        for lbd in self.cfg.args.lbds:
            top = int(lbd * self.TOT_SIZE)
            res[self.cfg.name_top(top)] = trade_day_data.head(top)[x].sum()
        return pd.Series(res)

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)

        # load adj major data as header
        maj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "oi_instru"],
        )

        # load member
        pos_data = self.load_pos(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=[
                "trade_date", "ts_code", "broker",
                "vol", "vol_chg", "long_hld", "long_chg", "short_hld", "short_chg",
                "code_type"
            ]
        )
        cntrct_pos_data = pos_data.query("code_type == 0 and broker != '期货公司' and broker != '非期货公司'")
        pos_lng_data = cntrct_pos_data[["trade_date", "ts_code", "broker", "long_hld", "long_chg"]].dropna(
            axis=0, how="any", subset=["long_hld", "long_chg"]).sort_values(
            by=["trade_date", "long_hld"], ascending=[True, False],
        )
        pos_srt_data = cntrct_pos_data[["trade_date", "ts_code", "broker", "short_hld", "short_chg"]].dropna(
            axis=0, how="any", subset=["short_hld", "short_chg"]).sort_values(
            by=["trade_date", "short_hld"], ascending=[True, False],
        )
        pos_lng_chg_by_c = pos_lng_data.groupby(by=["trade_date", "ts_code"]).apply(self.cal_top, x="long_chg")
        pos_srt_chg_by_c = pos_srt_data.groupby(by=["trade_date", "ts_code"]).apply(self.cal_top, x="short_chg")
        pos_lng_chg: pd.DataFrame = pos_lng_chg_by_c.groupby(by="trade_date").sum()
        pos_srt_chg: pd.DataFrame = pos_srt_chg_by_c.groupby(by="trade_date").sum()

        res = {}
        for lbd in self.cfg.args.lbds:
            top = int(lbd * self.TOT_SIZE)
            name_top = self.cfg.name_top(top)
            d0: pd.Series = pos_lng_chg[name_top] - pos_srt_chg[name_top]
            d1: pd.Series = pos_lng_chg[name_top].abs() + pos_srt_chg[name_top].abs()
            for win in self.cfg.args.wins:
                name_vanilla = self.cfg.name_vanilla(win, lbd)
                res[name_vanilla] = d0.rolling(win).sum() / d1.rolling(win).sum()
        res_data = pd.DataFrame(res)
        maj_data = pd.merge(left=maj_data, right=res_data, on="trade_date", how="left")
        maj_data[self.cfg.factor_names] = maj_data[self.cfg.factor_names].fillna(0)
        self.rename_ticker(maj_data)
        factor_data = self.get_factor_data(maj_data, bgn_date)
        return factor_data
