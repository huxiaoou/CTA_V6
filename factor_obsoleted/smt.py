import numpy as np
import pandas as pd
from husfort.qcalendar import CCalendar
from typedefs.typedefFactors import CCfgFactorGrpWinLbd, TFactorNames
from solutions.factor import CFactorsByInstru
from math_tools.robust import robust_ret_alg


class CCfgFactorGrpSMT(CCfgFactorGrpWinLbd):
    def __init__(self, **kwargs):
        super().__init__(factor_class="SMT", **kwargs)

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla + self.names_delay


class CFactorSMT(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpSMT, **kwargs):
        if not isinstance(factor_grp, CCfgFactorGrpSMT):
            raise TypeError("factor_grp must be CCfgFactorGrpSMT")
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    @staticmethod
    def cal_smart_idx(data: pd.DataFrame, ret: str, vol: str) -> pd.Series:
        return data[[ret, vol]].apply(lambda z: np.abs(z[ret]) / np.log(z[vol]) * 1e4 if z[vol] > 1 else 0, axis=1)

    @staticmethod
    def cal_smt(sorted_sub_data: pd.DataFrame, lbd: float, prc: str = "vwap") -> float:
        # total price and ret
        if (tot_amt_sum := sorted_sub_data["amount"].sum()) > 0:
            tot_w = sorted_sub_data["amount"] / tot_amt_sum
            tot_prc = sorted_sub_data[prc] @ tot_w
        else:
            return np.nan

        # select smart data from total
        volume_threshold = sorted_sub_data["vol"].sum() * lbd
        n = sum(sorted_sub_data["vol"].cumsum() < volume_threshold) + 1
        smt_df = sorted_sub_data.head(n)

        # smart price and ret
        if (smt_amt_sum := smt_df["amount"].sum()) > 0:
            smt_w = smt_df["amount"] / smt_amt_sum
            smt_prc = smt_df[prc] @ smt_w
            smt_p = -((smt_prc / tot_prc - 1) * 1e4) if tot_prc > 0 else 0
            return smt_p
        else:
            return np.nan

    def cal_by_trade_date(self, trade_date_data: pd.DataFrame) -> pd.Series:
        res = {}
        for lbd, name_lbd in zip(self.cfg.args.lbds, self.cfg.names_lbd):
            res[name_lbd] = self.cal_smt(trade_date_data, lbd=lbd)
        return pd.Series(res)

    def cal_factor_by_instru(
            self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar
    ) -> pd.DataFrame:
        buffer_bgn_date = self.cfg.buffer_bgn_date(bgn_date, calendar)

        adj_major_data = self.load_preprocess(
            instru, bgn_date=buffer_bgn_date, stp_date=stp_date,
            values=["trade_date", "ticker_major"],
        )
        adj_minb_data = self.load_minute_bar(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
        adj_minb_data["freq_ret"] = robust_ret_alg(adj_minb_data["close"], adj_minb_data["pre_close"])
        adj_minb_data["freq_ret"] = adj_minb_data["freq_ret"].fillna(0)

        # contract multiplier is not considered when calculating "vwap"
        # because a price ratio is considered in the final results, not an absolute value of price is considered
        adj_minb_data["vwap"] = (adj_minb_data["amount"] / adj_minb_data["vol"]).ffill()

        # smart idx
        adj_minb_data["smart_idx"] = self.cal_smart_idx(adj_minb_data, ret="freq_ret", vol="vol")
        adj_minb_data = adj_minb_data.sort_values(by=["trade_date", "smart_idx"], ascending=[True, False])
        concat_factor_data = adj_minb_data.groupby(by="trade_date", group_keys=False).apply(self.cal_by_trade_date)
        for lbd, name_lbd in zip(self.cfg.args.lbds, self.cfg.names_lbd):
            for win in self.cfg.args.wins:
                name_vanilla, name_delay = self.cfg.name_vanilla(win, lbd), self.cfg.name_delay(win, lbd)
                concat_factor_data[name_vanilla] = concat_factor_data[name_lbd].rolling(window=win).mean()
                concat_factor_data[name_delay] = concat_factor_data[name_vanilla].shift(1)
        input_data = pd.merge(
            left=adj_major_data,
            right=concat_factor_data,
            left_on="trade_date",
            right_index=True,
            how="left",
        )
        self.rename_ticker(input_data)
        factor_data = self.get_factor_data(input_data, bgn_date=bgn_date)
        return factor_data
