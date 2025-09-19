import numpy as np
import pandas as pd
import multiprocessing as mp
from typing import Final
from rich.progress import Progress, TaskID, TimeElapsedColumn, TimeRemainingColumn, TextColumn, BarColumn
from husfort.qsqlite import CMgrSqlDb, CDbStruct
from husfort.qcalendar import CCalendar
from husfort.qutility import check_and_makedirs, error_handler
from husfort.qlog import logger
from typedefs.typedefFactors import CCfgFactorGrp, CFactor
from typedefs.typedefStrategies import CStrategy
from solutions.factor import CFactorsLoader
from solutions.optimize import COptimizerForStrategyReader
from solutions.shared import gen_sig_fac_db, gen_sig_strategy_db
from solutions.icov import get_cov_at_trade_date
from math_tools.weighted import gen_exp_wgt
from math_tools.weighted import adjust_weights


class CSignals:
    def __init__(self, signals_dir: str, signal_id: str):
        self.signals_dir = signals_dir
        self.signal_id = signal_id

    def get_sig_db_struct(self) -> CDbStruct:
        raise NotImplementedError

    def load(self, bgn_date: str, stp_date: str, value_columns: list[str] = None) -> pd.DataFrame:
        db_struct = self.get_sig_db_struct()
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct.db_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date, value_columns=value_columns)
        return data

    def save(self, data: pd.DataFrame, calendar: CCalendar):
        """

        :param data: a pd.DataFrame with columns = ["trade_date", "instrument"] + [vars]
        :param calendar:
        :return:
        """
        check_and_makedirs(self.signals_dir)
        db_struct = self.get_sig_db_struct()
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct.db_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="a",
        )
        if sqldb.check_continuity(incoming_date=data["trade_date"].iloc[0], calendar=calendar) == 0:
            sqldb.update(update_data=data)
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        raise NotImplementedError


class CSignalsUniFactor(CSignals):
    def __init__(self, factor: CFactor, signals_factors_dir: str):
        super().__init__(signals_dir=signals_factors_dir, signal_id=factor.factor_class)
        self.factor = factor

    def get_sig_db_struct(self) -> CDbStruct:
        return gen_sig_fac_db(
            save_dir=self.signals_dir,
            factor_class=self.factor.factor_class,
            factors=[self.factor],
        )


class CSignalsFactors(CSignals):
    def __init__(
            self,
            factor_grp: CCfgFactorGrp,
            factors_avlb_dir: str,
            signals_factors_dir: str,
    ):
        super().__init__(signals_dir=signals_factors_dir, signal_id=factor_grp.factor_class)
        self.factor_grp = factor_grp
        self.factors_avlb_dir = factors_avlb_dir

    def load_factors(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        factors_loader = CFactorsLoader(
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
            factors_avlb_dir=self.factors_avlb_dir,
        )
        return factors_loader.load(bgn_date, stp_date)

    def get_sig_db_struct(self) -> CDbStruct:
        return gen_sig_fac_db(
            save_dir=self.signals_dir,
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
        )

    def core(self, data: pd.DataFrame, rate: float, pb: Progress, task: TaskID) -> pd.DataFrame:
        wgt = gen_exp_wgt(k=len(data), rate=rate)
        res = {}
        for factor in self.factor_grp.factor_names:
            factor_data = data[[factor, "instrument"]].sort_values(by=factor, ascending=False)
            res[factor] = pd.Series(data=wgt, index=factor_data["instrument"])
        res = pd.DataFrame(res)
        pb.update(task_id=task, advance=1)
        return res

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        factor_data = self.load_factors(bgn_date, stp_date)
        with Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
        ) as pb:
            task = pb.add_task(description=self.signal_id)
            pb.update(task_id=task, completed=0, total=len(factor_data["trade_date"].unique()))
            weight_data = factor_data.groupby(by="trade_date").apply(  # type:ignore
                self.core, rate=1.0, pb=pb, task=task,
            )
        weight_data = weight_data.reset_index()
        save_data = pd.merge(
            left=factor_data[["trade_date", "instrument"]],
            right=weight_data,
            on=["trade_date", "instrument"],
            how="left",
        )
        self.save(save_data, calendar)
        return 0


class CSignalsStrategy(CSignals):
    def __init__(
            self,
            strategy: CStrategy,
            signals_strategies_dir: str,
            signals_factors_dir: str,
            optimize_dir: str,
            icov_data: pd.DataFrame,
            db_struct_css: CDbStruct,
    ):
        super().__init__(signals_dir=signals_strategies_dir, signal_id=strategy.name)
        self.strategy = strategy
        self.signals_factors_dir = signals_factors_dir
        self.optimize_dir = optimize_dir
        self.icov_data: Final[pd.DataFrame] = icov_data
        self.db_struct_css = db_struct_css

    def get_buffer_bgn_date(self, bgn_date: str, calendar: CCalendar) -> str:
        return calendar.get_next_date(bgn_date, shift=-self.strategy.ret.win + 1)

    def get_sig_db_struct(self) -> CDbStruct:
        return gen_sig_strategy_db(
            save_dir=self.signals_dir,
            save_id=self.signal_id,
        )

    def load_signals_factor(self, factor: CFactor, bgn_date: str, stp_date: str) -> pd.Series:
        """

        :param factor:
        :param bgn_date:
        :param stp_date:
        :return: a pd.Series with index = ["trade_date", "instrument"]
        """
        uni_fac_reader = CSignalsUniFactor(
            factor=factor,
            signals_factors_dir=self.signals_factors_dir,
        )
        data = uni_fac_reader.load(bgn_date, stp_date, value_columns=["trade_date", "instrument", factor.factor_name])
        return data.set_index(["trade_date", "instrument"])[factor.factor_name]

    def load_signals_factors(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        """

        :param bgn_date:
        :param stp_date:
        :return: a pd.DataFrame with columns = ["trade_date", "instrument"] + [factors]
        """
        factor_data = {}
        for factor in self.strategy.factors:
            factor_data[factor.factor_name] = self.load_signals_factor(factor, bgn_date, stp_date)
        factor_data = pd.DataFrame(factor_data)
        return factor_data.fillna(0).reset_index()

    def mov_ave(self, signals_factors: pd.DataFrame) -> pd.DataFrame:
        grp_keys = ["instrument"]
        ma_data = signals_factors.groupby(by=grp_keys)[self.strategy.factor_names].rolling(
            window=self.strategy.ret.win).mean().reset_index(level=grp_keys)
        result = pd.merge(
            left=signals_factors[["trade_date", "instrument"]],
            right=ma_data[self.strategy.factor_names],
            how="left",
            left_index=True,
            right_index=True,
        )
        return result

    def load_opt_wgt_for_factors(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        """

        :param bgn_date:
        :param stp_date:
        :return: a pd.DataFrame with columns = ["trade_date"] + [factors]
        """
        opt_wgt_reader = COptimizerForStrategyReader(strategy=self.strategy, optimize_dir=self.optimize_dir)
        opt_wgt_df = opt_wgt_reader.load(bgn_date, stp_date)
        return opt_wgt_df

    def cal_wgt_strategy(self, signals_ma: pd.DataFrame, opt_wgt: pd.DataFrame) -> pd.DataFrame:
        sig = signals_ma.set_index("trade_date")
        wgt = opt_wgt.set_index("trade_date")
        names = self.strategy.factor_names
        wsum = (sig[names] * wgt[names]).sum(axis=1)
        wsum_norm = wsum.groupby(level=0, group_keys=False).apply(lambda z: z / z.abs().sum())
        sig["weight"] = wsum_norm
        result = sig.reset_index()[["trade_date", "instrument", "weight"]]
        return result

    def load_tot_wgt(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_css.db_save_dir,
            db_name=self.db_struct_css.db_name,
            table=self.db_struct_css.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date)
        return data

    def core(self, data: pd.DataFrame, pb: Progress = None, task: TaskID = None, weight: str = "weight") -> pd.Series:
        trade_date = data["trade_date"].iloc[0]
        instruments = data["instrument"].tolist()
        covariance = get_cov_at_trade_date(self.icov_data, trade_date, instruments)
        k = len(data)
        factor_data = data[[weight, "instrument"]].sort_values(by=weight, ascending=False)
        w0 = factor_data["weight"].to_numpy()
        k0 = len(w0[w0 >= 0])
        k1 = k - k0
        top_list = factor_data["instrument"].head(k0).tolist()
        btm_list = factor_data["instrument"].tail(k1).tolist()
        cov_top = covariance.loc[top_list, top_list]
        cov_btm = covariance.loc[btm_list, btm_list]
        w_top, w_btm = w0[0:k0], w0[-k1:]
        var_top, var_btm = w_top @ cov_top @ w_top, w_btm @ cov_btm @ w_btm
        top_btm_ratio = np.sqrt(var_top / var_btm)
        w0[-k1:] = w0[-k1:] * top_btm_ratio
        w0 = w0 / np.sum(np.abs(w0))
        res = pd.Series(data=w0, index=factor_data["instrument"]).sort_index()
        pb.update(task_id=task, advance=1)
        return res

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        buffer_bgn_date = self.get_buffer_bgn_date(bgn_date, calendar)
        signals_factors = self.load_signals_factors(buffer_bgn_date, stp_date)
        signals_ma = self.mov_ave(signals_factors)
        opt_wgt = self.load_opt_wgt_for_factors(buffer_bgn_date, stp_date)
        raw_weights = self.cal_wgt_strategy(signals_ma, opt_wgt)
        raw_weights = raw_weights.query(f"trade_date >= '{bgn_date}'")
        with Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
        ) as pb:
            task = pb.add_task(description=self.signal_id)
            pb.update(task_id=task, completed=0, total=len(raw_weights["trade_date"].unique()))
            vol_adj_weights = raw_weights.groupby(by="trade_date").apply(self.core, pb=pb, task=task)
        vol_adj_weights = vol_adj_weights.reset_index().rename(columns={0: "weight"})
        tot_wgt = self.load_tot_wgt(bgn_date, stp_date)
        weights = adjust_weights(vol_adj_weights, tot_wgt)
        self.save(weights, calendar)
        return 0


# ----------------------
# --- main interface ---
# ----------------------

def main_signals(
        signals: list[CSignals],
        bgn_date: str,
        stp_date: str,
        calendar: CCalendar,
        call_multiprocess: bool,
        processes: int,
        desc: str,
):
    if call_multiprocess:
        with mp.get_context("spawn").Pool(processes=processes) as pool:
            for s in signals:
                pool.apply_async(
                    s.main,
                    kwds={
                        "bgn_date": bgn_date,
                        "stp_date": stp_date,
                        "calendar": calendar,
                    },
                    error_callback=error_handler,
                )
            pool.close()
            pool.join()
    else:
        for s in signals:
            s.main(bgn_date, stp_date, calendar)
    logger.info(f"Task {desc} accomplished.")
    return 0


# --- for factors ---
def gen_signals_from_factors(
        factor_cfgs: list[CCfgFactorGrp],
        factors_avlb_dir: str,
        signals_factors_dir: str,
) -> list[CSignalsFactors]:
    return [
        CSignalsFactors(
            factor_grp=z,
            factors_avlb_dir=factors_avlb_dir,
            signals_factors_dir=signals_factors_dir,
        )
        for z in factor_cfgs
    ]


# --- for strategies ---
def gen_signals_from_strategies(
        strategies: list[CStrategy],
        signals_strategies_dir: str,
        signals_factors_dir: str,
        optimize_dir: str,
        icov_data: pd.DataFrame,
        db_struct_css: CDbStruct,
) -> list[CSignalsStrategy]:
    return [
        CSignalsStrategy(
            strategy=z,
            signals_strategies_dir=signals_strategies_dir,
            signals_factors_dir=signals_factors_dir,
            optimize_dir=optimize_dir,
            icov_data=icov_data,
            db_struct_css=db_struct_css,
        )
        for z in strategies
    ]
