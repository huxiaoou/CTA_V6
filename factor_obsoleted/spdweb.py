import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWinLbd, TFactorNames
from solutions.factor import CFactorsByInstru


class CCfgFactorGrpSPDWEB(CCfgFactorGrpWinLbd):
    def __init__(self, **kwargs):
        super().__init__(factor_class="SPDWEB", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_diff


class CFactorSPDWEB(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpSPDWEB, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpSPDWEB):
            raise TypeError("factor_grp must be CCfgFactorGrpSPDWEB")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_spdweb(self, trade_date_data: pd.DataFrame) -> pd.Series:
        n = len(trade_date_data)
        res = {}
        for lbd, name_lbd in zip(self.cfg.args.lbds, self.cfg.names_lbd):
            k = max(int(n * lbd), 1)
            its = trade_date_data.head(k)["trd_senti"].mean()
            uts = trade_date_data.tail(k)["trd_senti"].mean()
            res[name_lbd] = its - uts
        return pd.Series(res)

    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)

        # load adj major data as header
        adj_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major", "oi_instru"],
        )

        # load member
        pos_data = self.load_pos(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=[
                "trade_date", "ts_code", "broker",
                "vol", "long_hld", "long_chg", "short_hld", "short_chg",
                "code_type"
            ]
        )

        cntrct_pos_data = pos_data.query("code_type == 0 and long_hld > 50 and short_hld > 50").dropna(
            axis=0, how="any", subset=["vol", "long_hld", "long_chg", "short_hld", "short_chg"],
        )
        cntrct_pos_data["stat"] = (cntrct_pos_data["long_hld"] + cntrct_pos_data["short_hld"]) / cntrct_pos_data["vol"]
        cntrct_pos_data["abs_chg_sum"] = cntrct_pos_data["long_chg"].abs() + cntrct_pos_data["short_chg"].abs()
        cntrct_pos_data["dlt_chg"] = cntrct_pos_data["long_chg"] - cntrct_pos_data["short_chg"]
        cntrct_pos_data["trd_senti"] = cntrct_pos_data["dlt_chg"] / cntrct_pos_data["abs_chg_sum"]
        cntrct_pos_data = cntrct_pos_data.sort_values(by=["trade_date", "stat"], ascending=[True, False])
        res_df = cntrct_pos_data.groupby(by="trade_date").apply(self.cal_spdweb).reset_index()
        for lbd, name_lbd in zip(self.cfg.args.lbds, self.cfg.names_lbd):
            for win in self.cfg.args.wins:
                name_vanilla = self.cfg.name_vanilla(win, lbd)
                res_df[name_vanilla] = res_df[name_lbd].rolling(window=win).mean()
        adj_data = pd.merge(left=adj_data, right=res_df, on="trade_date", how="left")
        na, nb = self.cfg.name_vanilla(win=20, lbd=0.9), self.cfg.name_vanilla(win=240, lbd=0.6)
        adj_data[self.cfg.name_diff()] = adj_data[na] - adj_data[nb]
        self.rename_ticker(adj_data)
        factor_data = self.get_factor_data(adj_data, bgn_date)
        return factor_data
