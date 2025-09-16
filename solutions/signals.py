import pandas as pd
import multiprocessing as mp
from rich.progress import Progress, track
from husfort.qsqlite import CMgrSqlDb, CDbStruct
from husfort.qcalendar import CCalendar
from husfort.qutility import check_and_makedirs, error_handler
from typedefs.typedefFactors import CCfgFactorGrp, CFactor
from typedefs.typedefStrategies import CStrategy
from solutions.factor import CFactorsLoader
from solutions.optimize import COptimizerForStrategyReader
from solutions.shared import gen_sig_fac_db, gen_sig_strategy_db
from math_tools.weighted import map_to_weight, adjust_weights


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

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        factor_data = self.load_factors(bgn_date, stp_date)
        grp_data = factor_data.groupby(by="trade_date", group_keys=False)
        weight_data = grp_data[self.factor_grp.factor_names].apply(map_to_weight, rate=1.0)
        save_data = pd.merge(
            left=factor_data[["trade_date", "instrument"]],
            right=weight_data,
            left_index=True,
            right_index=True,
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
            db_struct_css: CDbStruct,
    ):
        super().__init__(signals_dir=signals_strategies_dir, signal_id=strategy.name)
        self.strategy = strategy
        self.signals_factors_dir = signals_factors_dir
        self.optimize_dir = optimize_dir
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

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        buffer_bgn_date = self.get_buffer_bgn_date(bgn_date, calendar)
        signals_factors = self.load_signals_factors(buffer_bgn_date, stp_date)
        signals_ma = self.mov_ave(signals_factors)
        opt_wgt = self.load_opt_wgt_for_factors(buffer_bgn_date, stp_date)
        raw_weights = self.cal_wgt_strategy(signals_ma, opt_wgt)
        raw_weights = raw_weights.query(f"trade_date >= '{bgn_date}'")
        tot_wgt = self.load_tot_wgt(bgn_date, stp_date)
        weights = adjust_weights(raw_weights, tot_wgt)
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
        with Progress() as pb:
            main_task = pb.add_task(description=desc, total=len(signals))
            with mp.get_context("spawn").Pool(processes=processes) as pool:
                for s in signals:
                    pool.apply_async(
                        s.main,
                        kwds={
                            "bgn_date": bgn_date,
                            "stp_date": stp_date,
                            "calendar": calendar,
                        },
                        callback=lambda _: pb.update(task_id=main_task, advance=1),
                        error_callback=error_handler,
                    )
                pool.close()
                pool.join()
    else:
        for s in track(signals, description=desc):
            s.main(bgn_date, stp_date, calendar)
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
        db_struct_css: CDbStruct,
) -> list[CSignalsStrategy]:
    return [
        CSignalsStrategy(
            strategy=z,
            signals_strategies_dir=signals_strategies_dir,
            signals_factors_dir=signals_factors_dir,
            optimize_dir=optimize_dir,
            db_struct_css=db_struct_css,
        )
        for z in strategies
    ]
