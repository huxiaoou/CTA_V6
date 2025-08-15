import os
import numpy as np
import pandas as pd
import scipy.stats as sps
import multiprocessing as mp
from itertools import product
from typing import Literal
from loguru import logger
from rich.progress import track, Progress
from husfort.qutility import SFG, SFY, error_handler, check_and_makedirs
from husfort.qsqlite import CDbStruct, CMgrSqlDb
from husfort.qcalendar import CCalendar
from husfort.qinstruments import CInstruMgr
from husfort.qplot import CPlotLines
from typedefs.typedefFactors import (
    CArgsWin, CArgsWinLbd, CArgsLbd,
    CCfgFactorGrp, CCfgFactorGrpWin, CCfgFactorGrpWinLbd, CCfgFactorGrpLbd,
    CDecay,
    TFactorClass, TFactors, TFactorName, CFactor,
)
from typedefs.typedefInstrus import TUniverse
from solutions.shared import gen_factors_by_instru_db, gen_factors_avlb_db
from math_tools.rolling import cal_rolling_top_corr


class _CFactorsByInstruDbOperator:
    def __init__(self, factor_grp: CCfgFactorGrp, factors_by_instru_dir: str):
        self.factor_grp = factor_grp
        self.factors_by_instru_dir: str = factors_by_instru_dir

    def get_instru_db(self, instru: str) -> CDbStruct:
        return gen_factors_by_instru_db(
            instru=instru,
            factors_by_instru_dir=self.factors_by_instru_dir,
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
        )

    def load_by_instru(self, instru: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        db_struct_instru = self.get_instru_db(instru)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_instru.db_save_dir,
            db_name=db_struct_instru.db_name,
            table=db_struct_instru.table,
            mode="r",
        )
        factor_data = sqldb.read_by_range(bgn_date, stp_date)
        factor_data[self.factor_grp.factor_names] = (
            factor_data[self.factor_grp.factor_names].astype(np.float64).fillna(np.nan)
        )
        return factor_data

    def save_by_instru(self, factor_data: pd.DataFrame, instru: str, calendar: CCalendar):
        """

        :param factor_data: a pd.DataFrame with first 2 columns must be = ["trade_date", "ticker"]
                  then followed by factor names
        :param instru:
        :param calendar:
        :return:
        """
        db_struct_instru = self.get_instru_db(instru)
        check_and_makedirs(db_struct_instru.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_instru.db_save_dir,
            db_name=db_struct_instru.db_name,
            table=db_struct_instru.table,
            mode="a",
        )
        if sqldb.check_continuity(factor_data["trade_date"].iloc[0], calendar) == 0:
            sqldb.update(factor_data[db_struct_instru.table.vars.names])
        return 0

    def get_factor_data(self, input_data: pd.DataFrame, bgn_date: str) -> pd.DataFrame:
        """

        :param input_data:
        :param bgn_date:
        :return: a pd.DataFrame with first 2 columns must be = ["trade_date", "ticker"]
                  then followed by factor names
        """
        input_data = input_data.query(f"trade_date >= '{bgn_date}'")
        factor_data = input_data[["trade_date", "ticker"] + self.factor_grp.factor_names]
        return factor_data

    @staticmethod
    def rename_ticker(data: pd.DataFrame, old_name: str = "ticker_major") -> None:
        data.rename(columns={old_name: "ticker"}, inplace=True)


class _CFactorsByInstruMoreDb(_CFactorsByInstruDbOperator):
    def __init__(
            self,
            factor_grp: CCfgFactorGrp,
            factors_by_instru_dir: str,
            universe: TUniverse,
            db_struct_preprocess: CDbStruct = None,
            db_struct_minute_bar: CDbStruct | None = None,
            db_struct_pos: CDbStruct | None = None,
            db_struct_forex: CDbStruct | None = None,
            db_struct_macro: CDbStruct | None = None,
            db_struct_mkt: CDbStruct | None = None,
            instru_mgr: CInstruMgr | None = None,
    ):
        super().__init__(factor_grp, factors_by_instru_dir)
        self.universe = universe
        self.db_struct_preprocess = db_struct_preprocess
        self.db_struct_minute_bar = db_struct_minute_bar
        self.db_struct_pos = db_struct_pos
        self.db_struct_forex = db_struct_forex
        self.db_struct_macro = db_struct_macro
        self.db_struct_mkt = db_struct_mkt
        self.instru_mgr = instru_mgr

    def load_preprocess(self, instru: str, bgn_date: str, stp_date: str, values: list[str] = None) -> pd.DataFrame:
        if self.db_struct_preprocess is not None:
            db_struct_instru = self.db_struct_preprocess.copy_to_another(another_db_name=f"{instru}.db")
            sqldb = CMgrSqlDb(
                db_save_dir=db_struct_instru.db_save_dir,
                db_name=db_struct_instru.db_name,
                table=db_struct_instru.table,
                mode="r",
            )
            return sqldb.read_by_range(bgn_date, stp_date, value_columns=values)
        else:
            raise ValueError("Argument 'db_struct_preprocess' must be provided")

    def load_minute_bar(self, instru: str, bgn_date: str, stp_date: str, values: list[str] = None) -> pd.DataFrame:
        if self.db_struct_minute_bar is not None:
            db_struct_instru = self.db_struct_minute_bar.copy_to_another(another_db_name=f"{instru}.db")
            sqldb = CMgrSqlDb(
                db_save_dir=db_struct_instru.db_save_dir,
                db_name=db_struct_instru.db_name,
                table=db_struct_instru.table,
                mode="r",
            )
            return sqldb.read_by_range(bgn_date, stp_date, value_columns=values)
        else:
            raise ValueError("Argument 'db_struct_minute_bar' must be provided")

    def load_pos(self, instru: str, bgn_date: str, stp_date: str, values: list[str] = None) -> pd.DataFrame:
        if self.db_struct_pos is not None:
            db_struct_instru = self.db_struct_pos.copy_to_another(another_db_name=f"{instru}.db")
            sqldb = CMgrSqlDb(
                db_save_dir=db_struct_instru.db_save_dir,
                db_name=db_struct_instru.db_name,
                table=db_struct_instru.table,
                mode="r",
            )
            return sqldb.read_by_range(bgn_date, stp_date, value_columns=values)
        else:
            raise ValueError("Argument 'db_struct_pos' must be provided")

    def load_forex(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        if self.db_struct_forex is not None:
            sqldb = CMgrSqlDb(
                db_save_dir=self.db_struct_forex.db_save_dir,
                db_name=self.db_struct_forex.db_name,
                table=self.db_struct_forex.table,
                mode="r",
            )
            return sqldb.read_by_range(bgn_date, stp_date)
        else:
            raise ValueError("Argument 'db_struct_forex' must be provided")

    def load_macro(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        if self.db_struct_macro is not None:
            sqldb = CMgrSqlDb(
                db_save_dir=self.db_struct_macro.db_save_dir,
                db_name=self.db_struct_macro.db_name,
                table=self.db_struct_macro.table,
                mode="r",
            )
            return sqldb.read_by_range(bgn_date, stp_date)
        else:
            raise ValueError("Argument 'db_struct_macro' must be provided")

    def load_mkt(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        if self.db_struct_mkt is not None:
            sqldb = CMgrSqlDb(
                db_save_dir=self.db_struct_mkt.db_save_dir,
                db_name=self.db_struct_mkt.db_name,
                table=self.db_struct_mkt.table,
                mode="r",
            )
            return sqldb.read_by_range(bgn_date, stp_date)
        else:
            raise ValueError("Argument 'db_struct_mkt' must be provided")


class CFactorsByInstru(_CFactorsByInstruMoreDb):
    def cal_factor_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        """
        This function is to be realized by specific factors

        :return : a pd.DataFrame with first 2 columns must be = ["trade_date", "ticker"]
                  then followed by factor names
        """
        raise NotImplementedError

    def get_default_factor_data(self) -> pd.DataFrame:
        return pd.DataFrame(columns=["trade_date", "ticker"] + self.factor_grp.factor_names)

    def process_by_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar):
        factor_data = self.cal_factor_by_instru(instru, bgn_date, stp_date, calendar)
        self.save_by_instru(factor_data, instru, calendar)
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar, call_multiprocess: bool, processes: int):
        description = f"Calculating factor {SFY(self.factor_grp.factor_class)}"
        if call_multiprocess:
            with Progress() as pb:
                main_task = pb.add_task(description, total=len(self.universe))
                with mp.get_context("spawn").Pool(processes) as pool:
                    for instru in self.universe:
                        pool.apply_async(
                            self.process_by_instru,
                            args=(instru, bgn_date, stp_date, calendar),
                            callback=lambda _: pb.update(main_task, advance=1),
                            error_callback=error_handler,
                        )
                    pool.close()
                    pool.join()
        else:
            for instru in track(self.universe, description=description):
                self.process_by_instru(instru, bgn_date, stp_date, calendar)
        return 0


class CFactorCORR(CFactorsByInstru):
    def __init__(self, factor_grp: CCfgFactorGrpWinLbd, **kwargs):
        super().__init__(factor_grp=factor_grp, **kwargs)
        self.cfg = factor_grp

    def cal_core(
            self,
            raw_data: pd.DataFrame,
            bgn_date: str, stp_date: str,
            x: str, y: str,
            sort_var: str, direction: int = -1,
    ):
        for win, lbd in product(self.cfg.args.wins, self.cfg.args.lbds):
            name_vanilla = self.cfg.name_vanilla(win, lbd)
            raw_data[name_vanilla] = cal_rolling_top_corr(
                raw_data=raw_data,
                bgn_date=bgn_date, stp_date=stp_date,
                win=win, top=lbd, x=x, y=y,
                sort_var=sort_var, direction=direction,
            )
        return 0


class CFactorsAvlb(_CFactorsByInstruDbOperator):
    def __init__(
            self,
            factor_grp: CCfgFactorGrp,
            universe: TUniverse,
            factors_by_instru_dir: str,
            factors_avlb_raw_dir: str,
            factors_avlb_ewa_dir: str,
            db_struct_avlb: CDbStruct,
    ):
        super().__init__(factor_grp, factors_by_instru_dir)
        self.universe = universe
        self.factors_avlb_raw_dir = factors_avlb_raw_dir
        self.factors_avlb_ewa_dir = factors_avlb_ewa_dir
        self.db_struct_avlb = db_struct_avlb

    def load_ref_fac(self, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = calendar.get_next_date(bgn_date, shift=-self.factor_grp.decay.win + 1)
        ref_dfs: list[pd.DataFrame] = []
        for instru in self.universe:
            df = self.load_by_instru(instru, bgn_date=buffer_bgn_date, stp_date=stp_date)
            df["instrument"] = instru
            ref_dfs.append(df)
        res = pd.concat(ref_dfs, axis=0, ignore_index=False)
        res = res.reset_index().sort_values(by=["trade_date"], ascending=True)
        res = res[["trade_date", "instrument"] + self.factor_grp.factor_names]
        return res

    def load_available(self, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        buffer_bgn_date = calendar.get_next_date(bgn_date, shift=-self.factor_grp.decay.win + 1)
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_avlb.db_save_dir,
            db_name=self.db_struct_avlb.db_name,
            table=self.db_struct_avlb.table,
            mode="r",
        )
        avlb_data = sqldb.read_by_range(bgn_date=buffer_bgn_date, stp_date=stp_date)
        avlb_data = avlb_data[["trade_date", "instrument", "sectorL1"]]
        return avlb_data

    def fillna_by_sector(self, avlb_i_data: pd.DataFrame) -> pd.DataFrame:
        grp_keys = ["trade_date", "sectorL1"]
        o_data = avlb_i_data.groupby(by=grp_keys)[self.factor_grp.factor_names].apply(
            lambda z: z.fillna(z.mean())
        ).reset_index(level=grp_keys)
        avlb_o_data = pd.merge(
            left=avlb_i_data[["trade_date", "instrument", "sectorL1"]],
            right=o_data[self.factor_grp.factor_names],
            how="inner",
            left_index=True, right_index=True,
        )
        if (l0 := len(avlb_i_data)) != (l1 := len(avlb_o_data)):
            raise ValueError(f"len of raw data = {l0} != len of fil data = {l1}.")
        return avlb_o_data

    def normalize(self, avlb_i_data: pd.DataFrame, q: float = 0.995) -> pd.DataFrame:
        def __normalize(data: pd.DataFrame) -> pd.DataFrame:
            # winsorize
            k = sps.norm.ppf(q)
            mu = data.mean()
            sd = data.std()
            ub, lb = mu + k * sd, mu - k * sd
            t = data.copy()
            for col in data.columns:
                t[col] = t[col].mask(t[col] > ub[col], other=ub[col])
                t[col] = t[col].mask(t[col] < lb[col], other=lb[col])

            # normalize
            z = (t - t.mean()) / t.std()
            return z

        grp_keys = ["trade_date"]
        o_data = avlb_i_data.groupby(by=grp_keys)[self.factor_grp.factor_names].apply(
            __normalize).reset_index(level=grp_keys)
        avlb_o_data = pd.merge(
            left=avlb_i_data[["trade_date", "instrument", "sectorL1"]],
            right=o_data[self.factor_grp.factor_names],
            how="inner",
            left_index=True, right_index=True,
        )
        if (l0 := len(avlb_i_data)) != (l1 := len(avlb_o_data)):
            raise ValueError(f"len of raw data = {l0}  != len of nrm data = {l1}.")
        return avlb_o_data

    def moving_average(self, avlb_i_data: pd.DataFrame) -> pd.DataFrame:
        def __mov_ave(data: pd.DataFrame, w: np.ndarray) -> pd.DataFrame:
            return data.rolling(window=win, min_periods=1).apply(
                lambda z: z @ w if len(z) == len(w) else z.mean()
            )

        grp_keys = ["instrument"]
        win, wgt = self.factor_grp.decay.win, self.factor_grp.decay.wgt
        o_data = avlb_i_data.groupby(by=grp_keys)[self.factor_grp.factor_names].apply(  # type:ignore
            __mov_ave, w=wgt).reset_index(level=grp_keys)
        avlb_o_data = pd.merge(
            left=avlb_i_data[["trade_date", "instrument", "sectorL1"]],
            right=o_data[self.factor_grp.factor_names],
            how="inner",
            left_index=True, right_index=True,
        )
        if (l0 := len(avlb_i_data)) != (l1 := len(avlb_o_data)):
            raise ValueError(f"len of raw data = {l0}  != len of neu data = {l1}.")
        return avlb_o_data

    def save(self, new_data: pd.DataFrame, calendar: CCalendar, save_type: Literal["raw", "ewa"]):
        if save_type == "raw":
            factors_avlb_dir = self.factors_avlb_raw_dir
        elif save_type == "ewa":
            factors_avlb_dir = self.factors_avlb_ewa_dir
        else:
            raise ValueError(f"Invalid save_type {save_type}")
        db_struct_fac = gen_factors_avlb_db(
            factors_avlb_dir=factors_avlb_dir,
            factor_class=self.factor_grp.factor_class,
            factors=self.factor_grp.factors,
        )
        check_and_makedirs(db_struct_fac.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_fac.db_save_dir,
            db_name=db_struct_fac.db_name,
            table=db_struct_fac.table,
            mode="a",
        )
        if sqldb.check_continuity(new_data["trade_date"].iloc[0], calendar) == 0:
            instru_tst_ret_agg_data = new_data[db_struct_fac.table.vars.names]
            sqldb.update(update_data=instru_tst_ret_agg_data)
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        logger.info(f"Calculate available factor {SFG(self.factor_grp.factor_class)}")
        # avlb raw
        ref_fac_data = self.load_ref_fac(bgn_date, stp_date, calendar)
        available_data = self.load_available(bgn_date, stp_date, calendar)
        fac_avlb_raw_data = pd.merge(
            left=available_data,
            right=ref_fac_data,
            on=["trade_date", "instrument"],
            how="left",
        ).sort_values(by=["trade_date", "sectorL1"])

        # avlb nrm
        logger.info(f"Fill and Normalize available factor {SFG(self.factor_grp.factor_class)}")
        fac_avlb_fil_data = self.fillna_by_sector(fac_avlb_raw_data)
        fac_avlb_nrm_data = self.normalize(fac_avlb_fil_data)
        save_avlb_nrm_data = fac_avlb_nrm_data.query(f"trade_date >= '{bgn_date}'")
        self.save(save_avlb_nrm_data, calendar, save_type="raw")

        # avlb ma
        logger.info(f"Moving average available factor {SFG(self.factor_grp.factor_class)}")
        fac_avlb_ma_data = self.moving_average(fac_avlb_nrm_data)
        save_avlb_ma_data = fac_avlb_ma_data.query(f"trade_date >= '{bgn_date}'")
        self.save(save_avlb_ma_data, calendar, save_type="ewa")

        logger.info(f"All done for factor {SFG(self.factor_grp.factor_class)}")
        return 0


class CFactorsLoader:
    def __init__(self, factor_class: TFactorClass, factors: TFactors, factors_avlb_dir: str):
        """

        :param factor_class:
        :param factors:
        :param factors_avlb_dir:  factors_avlb_raw_dir or factors_avlb_neu_dir
        """
        self.factor_class = factor_class
        self.factors = factors
        self.factors_avlb_dir = factors_avlb_dir

    @property
    def value_columns(self) -> list[str]:
        return ["trade_date", "instrument"] + [f.factor_name for f in self.factors]

    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        db_struct_fac = gen_factors_avlb_db(
            factors_avlb_dir=self.factors_avlb_dir,
            factor_class=self.factor_class,
            factors=self.factors,
        )
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_fac.db_save_dir,
            db_name=db_struct_fac.db_name,
            table=db_struct_fac.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date, value_columns=self.value_columns)
        return data


"""
------------------------------------
--- Management tools for factors ---
------------------------------------
"""


class CCfgFactors:
    def __init__(self, algs_dir: str, cfg_data: dict, decay: dict[str, int | float]):
        self.mgr: dict[str, tuple[CCfgFactorGrp, type[CFactorsByInstru]]] = {}
        for module in os.listdir(algs_dir):
            if module.endswith(".py"):
                module_name = module[:-3]  # exclude ".py"
                factor_class = module_name.upper()  # "mtm" -> "MTM"
                module_path = f"{algs_dir}.{module_name}"
                module_contents = __import__(module_path)
                type_cfg = getattr(module_contents.__dict__[module_name], f"CCfgFactorGrp{factor_class}")
                type_fac = getattr(module_contents.__dict__[module_name], f"CFactor{factor_class}")
                d = cfg_data[factor_class]
                d["decay"] = CDecay(**d.get("decay", decay))
                wins, lbds = d["args"].get("wins", None), d["args"].get("lbds", None)
                if type_cfg.__base__ == CCfgFactorGrpWin:
                    d["args"] = CArgsWin(wins=wins)
                elif type_cfg.__base__ == CCfgFactorGrpWinLbd:
                    d["args"] = CArgsWinLbd(wins=wins, lbds=lbds)
                elif type_cfg.__base__ == CCfgFactorGrpLbd:
                    d["args"] = CArgsLbd(lbds=lbds)
                else:
                    raise TypeError(f"Unsupported type: {type_cfg.__base__}")
                self.mgr[factor_class] = (type_cfg(**d), type_fac)

    def __repr__(self):
        r, fi = "", 0
        for factor_class, (cfg, fac) in self.mgr.items():
            r += f"{fi:>02d}:{factor_class:<10s}: ({cfg}, {fac})\n"
            fi += 1
        return r

    def get_cfgs(self) -> list[CCfgFactorGrp]:
        return [z[0] for z in self.mgr.values()]

    def get_cfg(self, factor_class: str) -> CCfgFactorGrp:
        return self.mgr[factor_class][0]

    def get_fac(self, factor_class: str) -> type[CFactorsByInstru]:
        return self.mgr[factor_class][1]

    def get_cfg_and_fac(self, factor_class: str) -> tuple[CCfgFactorGrp, type[CFactorsByInstru]]:
        return self.mgr[factor_class]

    @property
    def classes(self) -> list[str]:
        return list(self.mgr.keys())

    def match_class(self, factor_name: TFactorName) -> TFactorClass | None:
        for factor_class, (cfg, _) in self.mgr.items():
            if factor_name in cfg.factor_names:
                return TFactorClass(factor_class)
        raise ValueError(f"No factor named {factor_name}")

    def match_factor(self, factor_name: TFactorName) -> CFactor:
        factor_class = self.match_class(factor_name)
        factor = CFactor(factor_class, factor_name)
        return factor


def pick_factor(
        fclass: TFactorClass,
        cfg_factors: CCfgFactors,
        factors_by_instru_dir: str,
        universe: TUniverse,
        preprocess: CDbStruct,
        minute_bar: CDbStruct,
        db_struct_pos: CDbStruct,
        db_struct_forex: CDbStruct,
        db_struct_macro: CDbStruct,
        db_struct_mkt: CDbStruct,
        instru_mgr: CInstruMgr,
) -> tuple[CCfgFactorGrp, CFactorsByInstru]:
    cfg, fac_prototype = cfg_factors.get_cfg_and_fac(fclass)
    fac = fac_prototype(
        factor_grp=cfg,
        factors_by_instru_dir=factors_by_instru_dir,
        universe=universe,
        db_struct_preprocess=preprocess,
        db_struct_minute_bar=minute_bar,
        db_struct_pos=db_struct_pos,
        db_struct_forex=db_struct_forex,
        db_struct_macro=db_struct_macro,
        db_struct_mkt=db_struct_mkt,
        instru_mgr=instru_mgr,
    )
    return cfg, fac


"""
---------------------------------------------
--- check correlation between two factors ---
---------------------------------------------
"""


def cal_corr_2f(
        f0: CFactor, f1: CFactor, factors_avlb_dir: str,
        bgn_date: str, stp_date: str,
        factors_corr_dir: str
):
    if factors_avlb_dir.endswith("factors_avlb_raw"):
        raw_ma_tag = "raw"
    elif factors_avlb_dir.endswith("factors_avlb_ma"):
        raw_ma_tag = "ma"
    else:
        raise ValueError(f"factors_avlb_dir = {factors_avlb_dir} is illegal")
    save_id = f"ic_{f0.factor_name}_{f1.factor_name}_{raw_ma_tag}"

    # load data
    f0_loader = CFactorsLoader(f0.factor_class, factors=[f0], factors_avlb_dir=factors_avlb_dir)
    f1_loader = CFactorsLoader(f1.factor_class, factors=[f1], factors_avlb_dir=factors_avlb_dir)
    f0_data = f0_loader.load(bgn_date, stp_date)
    f1_data = f1_loader.load(bgn_date, stp_date)
    merged_data = pd.merge(
        left=f0_data, right=f1_data, how="inner", on=["trade_date", "instrument"]
    )

    # cal ic
    f_names = [f0.factor_name, f1.factor_name]
    ic = merged_data.groupby(by="trade_date")[f_names].apply(
        lambda z: z.corr().loc[f0.factor_name, f1.factor_name]
    )
    ic_cumsum = ic.cumsum()
    res = pd.DataFrame({"ic": ic, "ic_cumsum": ic_cumsum})

    # save to file
    check_and_makedirs(factors_corr_dir)
    res_file = f"{save_id}.csv"
    res_path = os.path.join(factors_corr_dir, res_file)
    res.to_csv(res_path, float_format="%.6f", index_label="trade_date")

    # plot
    artist = CPlotLines(
        plot_data=res[["ic_cumsum"]],
        fig_name=f"{save_id}",
        fig_save_dir=factors_corr_dir,
        colormap="jet"
    )
    artist.plot()
    artist.set_axis_x(xtick_count=20, xtick_label_size=8)
    artist.save_and_close()
    logger.info(f"Correlation between {f0.factor_name} and {f1.factor_name} calculated")
    return 0
