import numpy as np
import pandas as pd
from typing import Literal
from rich.progress import track
from husfort.qlog import logger
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CMgrSqlDb
from husfort.qutility import check_and_makedirs, SFG
from husfort.qoptimization import COptimizerPortfolioSharpe
from typedefs.typedefFactors import CFactor
from typedefs.typedefStrategies import CStrategy
from solutions.shared import gen_optimize_db, gen_vt_tests_db


class COptimizerForStrategyReader:
    def __init__(self, strategy: CStrategy, optimize_dir: str):
        self.strategy = strategy
        self.optimize_dir = optimize_dir

    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        db_struct = gen_optimize_db(self.optimize_dir, strategy=self.strategy)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct.db_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date)
        return data


class __COptimizerForStrategy(COptimizerForStrategyReader):
    WEEK_SPAN = 10

    def get_buffer_bgn_date(self, bgn_date: str, calendar: CCalendar) -> str:
        return calendar.get_next_date(bgn_date, shift=-self.strategy.opt_win - self.WEEK_SPAN + 1)

    def load_input(self, bgn_date: str, stp_date: str):
        raise NotImplementedError

    def optimize_at_day(self, trade_date: str, calendar: CCalendar) -> pd.Series:
        """

        :param trade_date:
        :param calendar:
        :return: a pd.Series with index = [factors]
        """
        raise NotImplementedError

    @staticmethod
    def align(weights: pd.DataFrame, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        """

        :param weights: with index = "trade_date"
        :param bgn_date:
        :param stp_date:
        :param calendar:
        :return:
        """
        header_dates = pd.DataFrame({
            "trade_date": calendar.get_iter_list(bgn_date=weights.index[0], stp_date=stp_date)  # type:ignore
        })
        data = pd.merge(
            left=header_dates,
            right=weights,
            left_on="trade_date",
            right_index=True,
            how="left",
        )
        filled_data = data.ffill(axis=0).query(f"trade_date >= '{bgn_date}' and trade_date < '{stp_date}'")
        return filled_data

    def save(self, data: pd.DataFrame, calendar: CCalendar):
        """

        :param data: a pd.DataFrame with columns = ["trade_date"] + [factors]
        :param calendar:
        :return:
        """
        check_and_makedirs(self.optimize_dir)
        db_struct = gen_optimize_db(self.optimize_dir, strategy=self.strategy)
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
        buffer_bgn_date = self.get_buffer_bgn_date(bgn_date, calendar)
        self.load_input(bgn_date=buffer_bgn_date, stp_date=stp_date)
        dates_opt = calendar.get_week_end_days_in_range(bgn_date=buffer_bgn_date, stp_date=stp_date)
        weights_opt: dict[str, pd.Series] = {}
        for date_opt in track(dates_opt, description=f"Optimizing strategies {SFG(self.strategy.name)}"):
            weights_opt[date_opt] = self.optimize_at_day(trade_date=date_opt, calendar=calendar)
        weights_opt: pd.DataFrame = pd.DataFrame.from_dict(weights_opt, orient="index")
        weights_aligned = self.align(weights=weights_opt, bgn_date=bgn_date, stp_date=stp_date, calendar=calendar)
        self.save(data=weights_aligned, calendar=calendar)
        return 0


class COptimizerForStrategyEQ(__COptimizerForStrategy):
    def load_input(self, bgn_date: str, stp_date: str):
        pass

    def optimize_at_day(self, trade_date: str, calendar: CCalendar) -> pd.Series:
        idx = [f.factor_name for f in self.strategy.factors]
        return pd.Series(data=1 / len(idx), index=idx)


class COptimizerForStrategyVT(__COptimizerForStrategy):
    def __init__(
            self,
            strategy: CStrategy,
            optimize_dir: str,
            vt_tests_dir: str,
            volatility_adjusted: bool,
    ):
        super().__init__(strategy, optimize_dir)
        self.vt_tests_dir = vt_tests_dir
        self.volatility_adjusted = volatility_adjusted
        self.vt_rets: pd.DataFrame = pd.DataFrame()

    def _load_factor_rets(self, factor: CFactor, bgn_date: str, stp_date: str) -> pd.Series:
        db_struct = gen_vt_tests_db(
            vt_tests_dir=self.vt_tests_dir,
            factor_class=factor.factor_class,
            factors=[factor],
            ret=self.strategy.ret,
            volatility_adjusted=self.volatility_adjusted,
        )
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct.db_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date, value_columns=["trade_date", factor.factor_name])
        return data.set_index("trade_date")[factor.factor_name]

    def _load_factors_rets(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        factors_rets: dict[str, pd.Series] = {}
        for factor in self.strategy.factors:
            factors_rets[factor.factor_name] = self._load_factor_rets(factor, bgn_date, stp_date)
        factors_rets: pd.DataFrame = pd.DataFrame(factors_rets)
        return factors_rets

    def load_input(self, bgn_date: str, stp_date: str):
        self.vt_rets = self._load_factors_rets(bgn_date, stp_date)

    @staticmethod
    def optimizer(rets: pd.DataFrame) -> pd.Series:
        sd = rets.std()
        sg = np.sign(rets.mean())
        w = sg / sd
        x0 = (w / w.abs().sum()).to_numpy()
        bounds = [(z * 0.8, z * 1.2) if z > 0 else (z * 1.2, z * 0.8) for z in x0]
        optimizer = COptimizerPortfolioSharpe(
            m=rets.mean().to_numpy(),
            v=rets.cov().to_numpy(),
            x0=x0,
            bounds=bounds,
            tot_mkt_val_bds=(0.90, 1.10),
        )
        res = optimizer.optimize()
        w = pd.Series(data=res.x, index=rets.columns)
        if optimizer.sharpe(x0) >= optimizer.sharpe(res.x):
            logger.warning("Optimization failed.")
        return w / w.abs().sum()

    def optimize_at_day(self, trade_date: str, calendar: CCalendar) -> pd.Series:
        opt_bgn_date = calendar.get_next_date(trade_date, shift=-self.strategy.opt_win + 1)
        opt_rets = self.vt_rets.query(f"trade_date >= '{opt_bgn_date}' and trade_date <= '{trade_date}'")
        opt_wgt = self.optimizer(rets=opt_rets)
        return opt_wgt


# ------------
# --- main ---
# ------------

def main_optimize(
        strategies: list[CStrategy],
        bgn_date: str, stp_date: str, calendar: CCalendar,
        method: Literal["EQ", "VT"],
        optimize_dir: str,
        vt_tests_dir: str,
):
    for strategy in strategies:
        if method == "EQ":
            optimizer = COptimizerForStrategyEQ(
                strategy=strategy,
                optimize_dir=optimize_dir,
            )
        elif method == "VT":
            optimizer = COptimizerForStrategyVT(
                strategy=strategy,
                optimize_dir=optimize_dir,
                vt_tests_dir=vt_tests_dir,
                volatility_adjusted=False,
            )
        else:
            raise ValueError(f"Invalid method: {method}")
        optimizer.main(bgn_date=bgn_date, stp_date=stp_date, calendar=calendar)
        logger.info(f"Optimizing strategy {SFG(strategy.name)} finished.")
