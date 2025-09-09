import os
import numpy as np
import pandas as pd
import multiprocessing as mp
from loguru import logger
from typing import Literal
from rich.progress import Progress, TaskID, TimeElapsedColumn, TimeRemainingColumn, TextColumn, BarColumn
from husfort.qutility import check_and_makedirs, SFG, qtimer, error_handler
from husfort.qsqlite import CMgrSqlDb, CDbStruct
from husfort.qcalendar import CCalendar
from husfort.qplot import CPlotLines
from husfort.qoptimization import COptimizerPortfolioSharpe
from typedefs.typedefReturns import CRet, TRets
from typedefs.typedefFactors import CCfgFactorGrp
from typedef import TFactorsAvlbDirType, TTestReturnsAvlbDirType
from math_tools.weighted import gen_exp_wgt, wic, map_to_weight
from solutions.test_return import CTestReturnLoader
from solutions.factor import CFactorsLoader
from solutions.shared import gen_ic_tests_db, gen_vt_tests_db
from solutions.icov import CICOVReader


class __CQTest:
    def __init__(
            self,
            factor_grp: CCfgFactorGrp,
            ret: CRet,
            factors_avlb_dir: str,
            test_returns_avlb_dir: str,
            db_struct_avlb: CDbStruct,
            tests_dir: str,
            volatility_adjusted: bool,
    ):
        self.factor_grp = factor_grp
        self.ret = ret
        self.factors_avlb_dir = factors_avlb_dir
        self.test_returns_avlb_dir = test_returns_avlb_dir
        self.db_struct_avlb = db_struct_avlb
        self.tests_dir = tests_dir
        self.volatility_adjusted = volatility_adjusted

    @property
    def save_id(self) -> str:
        fix_id = f"{self.factor_grp.factor_class}-{self.ret.ret_name}-{self.factor_grp.decay}"
        if self.volatility_adjusted:
            return f"{fix_id}-VA"
        else:
            return fix_id

    def load_returns(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        returns_loader = CTestReturnLoader(
            ret=self.ret,
            test_returns_avlb_dir=self.test_returns_avlb_dir,
        )
        return returns_loader.load(bgn_date, stp_date)

    def load_factors(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        factors_loader = CFactorsLoader(
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
            factors_avlb_dir=self.factors_avlb_dir,
        )
        return factors_loader.load(bgn_date, stp_date)

    def load_avlb(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_avlb.db_save_dir,
            db_name=self.db_struct_avlb.db_name,
            table=self.db_struct_avlb.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date, value_columns=["trade_date", "instrument", "volatility"])
        return data

    def load_other_data(self, bgn_date: str, stp_date: str):
        pass

    def gen_test_db_struct(self) -> CDbStruct:
        raise NotImplementedError

    def save(self, new_data: pd.DataFrame, calendar: CCalendar):
        """

        :param new_data: a pd.DataFrame with columns =
                        ["trade_date"] + self.factor_grp.factor_names
        :param calendar:
        :return:
        """
        test_db_struct = self.gen_test_db_struct()
        check_and_makedirs(test_db_struct.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=test_db_struct.db_save_dir,
            db_name=test_db_struct.db_name,
            table=test_db_struct.table,
            mode="a",
        )
        if sqldb.check_continuity(new_data["trade_date"].iloc[0], calendar) == 0:
            update_data = new_data[test_db_struct.table.vars.names]
            sqldb.update(update_data=update_data)
        return 0

    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        test_db_struct = self.gen_test_db_struct()
        check_and_makedirs(test_db_struct.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=test_db_struct.db_save_dir,
            db_name=test_db_struct.db_name,
            table=test_db_struct.table,
            mode="r",
        )
        data = sqldb.read_by_range(
            bgn_date=bgn_date, stp_date=stp_date,
            value_columns=["trade_date"] + self.factor_grp.factor_names,
        )
        return data

    def core(
            self, data: pd.DataFrame, volatility: str = "volatility", pb: Progress = None, task: TaskID = None,
    ) -> pd.Series:
        raise NotImplementedError

    def get_plot_ylim(self) -> tuple[float, float]:
        raise NotImplementedError

    def plot(self, plot_data: pd.DataFrame):
        check_and_makedirs(save_dir := os.path.join(self.tests_dir, "plots"))
        artist = CPlotLines(
            plot_data=plot_data,
            fig_name=f"{self.save_id}",
            fig_save_dir=save_dir,
            colormap="jet",
            line_style=["-", "-."] * int(plot_data.shape[1] / 2),
            line_width=1.2,
        )
        artist.plot()
        artist.set_legend(loc="upper left")
        artist.set_axis_x(xtick_count=20, xtick_label_size=8, xgrid_visible=True)
        artist.set_axis_y(ylim=self.get_plot_ylim(), update_yticklabels=False, ygrid_visible=True)
        artist.save_and_close()
        return 0

    def gen_report(self, test_data: pd.DataFrame, ret_scale: float = 100.0, ann_rate: float = 250) -> pd.DataFrame:
        raise NotImplementedError

    def save_report(self, report: pd.DataFrame, saving_index: bool, float_format: str = "%.6f"):
        check_and_makedirs(save_dir := os.path.join(self.tests_dir, "reports"))
        report_file = f"{self.save_id}.csv"
        report_path = os.path.join(save_dir, report_file)
        report.to_csv(report_path, float_format=float_format, index=saving_index)
        return 0

    def main_cal(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        buffer_bgn_date = calendar.get_next_date(bgn_date, -self.ret.shift)
        iter_dates = calendar.get_iter_list(buffer_bgn_date, stp_date)
        save_dates = iter_dates[self.ret.shift:]
        base_bgn_date, base_stp_date = iter_dates[0], iter_dates[-self.ret.shift]
        self.load_other_data(bgn_date=base_bgn_date, stp_date=base_stp_date)
        returns_data = self.load_returns(base_bgn_date, base_stp_date)
        factors_data = self.load_factors(base_bgn_date, base_stp_date)
        avlb_data = self.load_avlb(base_bgn_date, base_stp_date)
        input_data = pd.merge(
            left=returns_data,
            right=factors_data,
            on=["trade_date", "instrument"],
            how="inner",
        )
        lr, lf, li = len(returns_data), len(factors_data), len(input_data)
        if (li != lr) or (li != lf):
            raise ValueError(f"len of factor data = {lf}, len of return data = {lr}, len of input data = {li}.")
        input_data = input_data.merge(
            right=avlb_data,
            on=["trade_date", "instrument"],
            how="left",
        )
        with Progress(
                TextColumn("{task.description}"),
                BarColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
        ) as pb:
            task = pb.add_task(description=f"{self.save_id}")
            pb.update(task_id=task, completed=0, total=len(input_data["trade_date"].unique()))
            qtest_data = input_data.groupby(by="trade_date").apply(self.core, pb=pb, task=task)
        qtest_data["trade_date"] = save_dates
        new_data = qtest_data[["trade_date"] + self.factor_grp.factor_names]
        new_data = new_data.reset_index(drop=True)
        self.save(new_data, calendar)
        logger.info(f"{self.__class__.__name__} for {SFG(self.save_id)} finished.")
        return 0

    def main_summary(self, bgn_date: str, stp_date: str):
        test_data = self.load(bgn_date, stp_date).set_index("trade_date")
        plot_data = test_data.cumsum()
        self.plot(plot_data=plot_data)
        report = self.gen_report(test_data)
        self.save_report(report, saving_index=False)
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        self.main_cal(bgn_date, stp_date, calendar)
        self.main_summary(bgn_date, stp_date)
        return 0


# ----------------------------
# --------- ic-tests ---------
# ----------------------------
class CICTest(__CQTest):
    def core(
            self, data: pd.DataFrame, volatility: str = "volatility", pb: Progress = None, task: TaskID = None,
    ) -> pd.Series:
        if self.volatility_adjusted:
            data[volatility] = data[volatility].fillna(data[volatility].median())
            s = {}
            for factor in self.factor_grp.factor_names:
                s[factor] = wic(x=data[factor], y=data[self.ret.ret_name], w=1 / data[volatility])
            s = pd.Series(s)
        else:
            s = data[self.factor_grp.factor_names].corrwith(data[self.ret.ret_name], axis=0, method="spearman")
        pb.update(task_id=task, advance=1)
        return s

    def gen_test_db_struct(self) -> CDbStruct:
        return gen_ic_tests_db(
            ic_tests_dir=self.tests_dir,
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
            ret=self.ret,
            volatility_adjusted=self.volatility_adjusted,
        )

    def get_plot_ylim(self) -> tuple[float, float]:
        if self.ret.win <= 1:
            ylim = (-40, 80)
        elif self.ret.win <= 5:
            ylim = (-50, 100)
        elif self.ret.win <= 10:
            ylim = (-60, 120)
        else:
            ylim = (-80, 140)
        return ylim

    def gen_report(self, test_data: pd.DataFrame, ret_scale: float = 100.0, ann_rate: float = 250) -> pd.DataFrame:
        test_data["trade_year"] = test_data.index.map(lambda z: z[0:4])
        dfs: list[pd.DataFrame] = []
        for trade_year, trade_year_data in test_data.groupby("trade_year"):
            ic_mean = trade_year_data[self.factor_grp.factor_names].mean()
            ic_std = trade_year_data[self.factor_grp.factor_names].std()
            ir = ic_mean / ic_std
            trade_year_sum = pd.DataFrame(
                {
                    "trade_year": trade_year,
                    "IC": ic_mean,
                    "IR": ir,
                }
            ).reset_index().rename(columns={"index": "factor"})
            dfs.append(trade_year_sum)
        report = pd.concat(dfs, axis=0, ignore_index=True)
        return report


# ----------------------------
# --------- vt-tests ---------
# ----------------------------
class CVTTest(__CQTest):
    def core(
            self, data: pd.DataFrame, volatility: str = "volatility", pb: Progress = None, task: TaskID = None,
    ) -> pd.Series:
        wgt = gen_exp_wgt(k=len(data), rate=0.25)
        s = {}
        if self.volatility_adjusted:
            data[volatility] = data[volatility].fillna(data[volatility].median())
            for factor in self.factor_grp.factor_names:
                factor_data = data[[factor, volatility, self.ret.ret_name]].sort_values(by=factor, ascending=False)
                w0 = wgt / factor_data[volatility]
                w = w0 / w0.abs().sum()
                s[factor] = factor_data[self.ret.ret_name] @ w / self.ret.win
        else:
            for factor in self.factor_grp.factor_names:
                factor_data = data[[factor, self.ret.ret_name]].sort_values(by=factor, ascending=False)
                w = wgt
                s[factor] = factor_data[self.ret.ret_name] @ w / self.ret.win
        s = pd.Series(s)
        pb.update(task_id=task, advance=1)
        return s

    def gen_test_db_struct(self) -> CDbStruct:
        return gen_vt_tests_db(
            vt_tests_dir=self.tests_dir,
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
            ret=self.ret,
            volatility_adjusted=self.volatility_adjusted,
        )

    def get_plot_ylim(self) -> tuple[float, float]:
        return -0.2, 0.6

    def gen_report(self, test_data: pd.DataFrame, ret_scale: float = 100.0, ann_rate: float = 250) -> pd.DataFrame:
        test_data["trade_year"] = test_data.index.map(lambda z: z[0:4])
        dfs: list[pd.DataFrame] = []
        for trade_year, trade_year_data in test_data.groupby("trade_year"):
            vt_mean = trade_year_data[self.factor_grp.factor_names].mean() * ret_scale
            vt_std = trade_year_data[self.factor_grp.factor_names].std() * ret_scale
            ann_ret = vt_mean * ann_rate
            ann_vol = vt_std * np.sqrt(ann_rate)
            sharpe = ann_ret / ann_vol
            trade_year_sum = pd.DataFrame(
                {
                    "trade_year": trade_year,
                    "mean": vt_mean,
                    "std": vt_std,
                    "ann_ret": ann_ret,
                    "ann_vol": ann_vol,
                    "sharpe": sharpe,
                }
            ).reset_index().rename(columns={"index": "factor"})
            dfs.append(trade_year_sum)
        report = pd.concat(dfs, axis=0, ignore_index=True)
        return report


class COTTest(CVTTest):
    def __init__(self, icov_dir: str, **kwargs):
        super().__init__(**kwargs)
        self.icov_reader = CICOVReader(icov_db_dir=icov_dir)
        self.icov_data: pd.DataFrame = pd.DataFrame()

    def load_other_data(self, bgn_date: str, stp_date: str):
        self.icov_data = self.icov_reader.read(bgn_date, stp_date)

    def get_cov_at_trade_date(self, trade_date: str, instruments: list[str]) -> pd.DataFrame:
        trade_date_icov = self.icov_data.query(f"trade_date == '{trade_date}'")
        partial_cov = trade_date_icov.pivot(
            index="instrument0", columns="instrument1", values="cov",
        ).fillna(0).loc[instruments, instruments]
        variance = pd.DataFrame(data=np.diag(np.diag(partial_cov)), index=partial_cov.index,
                                columns=partial_cov.columns)
        instrus_cov = partial_cov + partial_cov.T - variance
        return instrus_cov

    def core(
            self, data: pd.DataFrame, volatility: str = "volatility", pb: Progress = None, task: TaskID = None,
    ) -> pd.Series:
        trade_date = data["trade_date"].iloc[0]
        instruments = data["instrument"].tolist()
        covariance = self.get_cov_at_trade_date(trade_date, instruments)
        k = len(data)
        s = {}
        for factor in self.factor_grp.factor_names:
            x0 = map_to_weight(data[factor], rate=0.25)
            # data['x0'] = x0
            # data[[factor, 'x0']].sort_values(by=[factor], ascending=False)
            # breakpoint()
            # (x0.min(), x0.max())
            optimizer = COptimizerPortfolioSharpe(
                m=data[factor].to_numpy() * 0.01,
                v=covariance.to_numpy(),
                x0=x0.to_numpy(),
                bounds=[(-3 / k, 3 / k)] * k,
                tot_mkt_val_bds=(0.8, 1.2),
                tol=1e-4,
                using_jac=False,
                verbose=False,
                ignore_warnings=True,
            )
            res = optimizer.optimize()
            wv, fv = res.x, res.fun
            # (res.x.min(), res.x.max())
            # print(pd.Series(wv).round(6))
            # print(f"sharpe(x0) = {optimizer.sharpe(x0.to_numpy()):.6f}")
            # print(f"sharpe(wv) = {optimizer.sharpe(wv):.6f}")
            # print(np.sum(np.abs(wv)))
            wv = wv / np.abs(wv).sum()
            # breakpoint()
            s[factor] = data[self.ret.ret_name] @ wv / self.ret.win
        s = pd.Series(s)
        pb.update(task_id=task, advance=1)
        return s


# --------------------------
# --- interface for main ---
# --------------------------
TICTestAuxArgs = tuple[TFactorsAvlbDirType, TTestReturnsAvlbDirType]


@qtimer
def main_qtests(
        rets: TRets,
        factor_grp: CCfgFactorGrp,
        aux_args_list: list[TICTestAuxArgs],
        db_struct_avlb: CDbStruct,
        tests_dir: str,
        icov_dir: str,
        bgn_date: str,
        stp_date: str,
        calendar: CCalendar,
        test_type: Literal["ic", "vt", "ot"],
        volatility_adjusted: bool,
        call_multiprocess: bool,
):
    extra_args = {}
    if test_type == "ic":
        test_cls = CICTest
    elif test_type == "vt":
        test_cls = CVTTest
    elif test_type == "ot":
        test_cls = COTTest
        extra_args.update({"icov_dir": icov_dir})
    else:
        raise ValueError("test_type must be in ['ic', 'vt', 'ot']")

    tests: list[__CQTest] = []
    for ret in rets:
        for factors_avlb_dir, test_returns_avlb_dir in aux_args_list:
            kwargs = {
                "factor_grp": factor_grp,
                "ret": ret,
                "factors_avlb_dir": factors_avlb_dir,
                "test_returns_avlb_dir": test_returns_avlb_dir,
                "db_struct_avlb": db_struct_avlb,
                "tests_dir": tests_dir,
                "volatility_adjusted": volatility_adjusted,
            }
            kwargs.update(extra_args)
            test = test_cls(**kwargs)
            tests.append(test)

    if call_multiprocess:
        with mp.get_context("spawn").Pool() as pool:
            for test in tests:
                pool.apply_async(
                    test.main,
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
        for test in tests:
            test.main(bgn_date, stp_date, calendar)
    return 0
