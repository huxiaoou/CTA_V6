import pandas as pd
from rich.progress import track
from loguru import logger
from husfort.qutility import SFG, check_and_makedirs
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CDbStruct, CMgrSqlDb
from husfort.qsimquick import CTestReturnLoaderBase
from solutions.shared import gen_test_returns_by_instru_db, gen_test_returns_avlb_db
from typedefs.typedefInstrus import TUniverse
from typedefs.typedefReturns import CRet, TReturnClass


class __CTestReturnsByInstru:
    def __init__(
            self,
            ret: CRet,
            universe: TUniverse,
            test_returns_by_instru_dir: str,
            db_struct_preprocess: CDbStruct
    ):
        self.ret = ret
        self.universe = universe
        self.test_returns_by_instru_dir = test_returns_by_instru_dir
        self.db_struct_preprocess = db_struct_preprocess

    def load_preprocess(self, instru: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_preprocess.db_save_dir,
            db_name=f"{instru}.db",
            table=self.db_struct_preprocess.table,
            mode="r",
        )
        data = sqldb.read_by_range(
            bgn_date=bgn_date, stp_date=stp_date,
            value_columns=["trade_date", "ticker_major", "return_c_major", "return_o_major"]
        )
        return data

    def core(self, ret: pd.Series) -> pd.Series:
        raise NotImplementedError

    def cal_test_return(
            self,
            instru_ret_data: pd.DataFrame,
            base_bgn_date: str,
            base_end_date: str,
    ) -> pd.DataFrame:
        if self.ret.ret_class == TReturnClass.CLS:
            raw_ret = "return_c_major"
        elif self.ret.ret_class == TReturnClass.OPN:
            raw_ret = "return_o_major"
        else:
            raise ValueError(f"Invalid ret_class: {self.ret.ret_class}")

        instru_ret_data[self.ret.ret_name] = self.core(ret=instru_ret_data[raw_ret])
        res = instru_ret_data.query(f"trade_date >= '{base_bgn_date}' & trade_date <= '{base_end_date}'")
        res = res[["trade_date", "ticker_major", self.ret.ret_name]]
        return res

    def process_for_instru(self, instru: str, bgn_date: str, stp_date: str, calendar: CCalendar):
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        base_bgn_date = calendar.get_next_date(iter_dates[0], -self.ret.shift)
        base_end_date = calendar.get_next_date(iter_dates[-1], -self.ret.shift)
        db_struct_instru = gen_test_returns_by_instru_db(
            instru=instru,
            test_returns_by_instru_dir=self.test_returns_by_instru_dir,
            ret_class=self.ret.ret_class,
            ret=self.ret,
        )
        check_and_makedirs(db_struct_instru.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_instru.db_save_dir,
            db_name=db_struct_instru.db_name,
            table=db_struct_instru.table,
            mode="a",
        )
        if sqldb.check_continuity(base_bgn_date, calendar) == 0:
            instru_ret_data = self.load_preprocess(instru, base_bgn_date, stp_date)
            y_instru_data = self.cal_test_return(instru_ret_data, base_bgn_date, base_end_date)
            sqldb.update(update_data=y_instru_data)
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        desc = f"Processing test return {SFG(self.ret.ret_name)}"
        for instru in track(self.universe, description=desc):
            self.process_for_instru(instru, bgn_date=bgn_date, stp_date=stp_date, calendar=calendar)
        return 0


class CTestReturnsByInstru(__CTestReturnsByInstru):
    def core(self, ret: pd.Series) -> pd.Series:
        ret_sum = ret.rolling(window=self.ret.win).sum()
        return ret_sum.shift(-self.ret.shift)


class CTestReturnsAvlb:
    def __init__(
            self,
            ret: CRet,
            universe: TUniverse,
            test_returns_by_instru_dir: str,
            test_returns_avlb_raw_dir: str,
            db_struct_avlb: CDbStruct,
    ):
        self.ret = ret
        self.universe = universe
        self.test_returns_by_instru_dir = test_returns_by_instru_dir
        self.test_returns_avlb_raw_dir = test_returns_avlb_raw_dir
        self.db_struct_avlb = db_struct_avlb

    def load_ref_ret_by_instru(self, instru: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        db_struct_ref = gen_test_returns_by_instru_db(
            instru=instru,
            test_returns_by_instru_dir=self.test_returns_by_instru_dir,
            ret_class=self.ret.ret_class,
            ret=self.ret,
        )
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_ref.db_save_dir,
            db_name=db_struct_ref.db_name,
            table=db_struct_ref.table,
            mode="r"
        )
        ref_data = sqldb.read_by_range(bgn_date, stp_date)
        return ref_data

    def load_ref_ret(self, base_bgn_date: str, base_stp_date: str) -> pd.DataFrame:
        ref_dfs: list[pd.DataFrame] = []
        for instru in self.universe:
            df = self.load_ref_ret_by_instru(instru, bgn_date=base_bgn_date, stp_date=base_stp_date)
            df["instrument"] = instru
            ref_dfs.append(df)
        res = pd.concat(ref_dfs, axis=0, ignore_index=False)
        res = res.reset_index().sort_values(by=["trade_date"], ascending=True)
        res = res[["trade_date", "instrument", self.ret.ret_name]]
        return res

    def load_available(self, base_bgn_date: str, base_stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_avlb.db_save_dir,
            db_name=self.db_struct_avlb.db_name,
            table=self.db_struct_avlb.table,
            mode="r",
        )
        avlb_data = sqldb.read_by_range(bgn_date=base_bgn_date, stp_date=base_stp_date)
        avlb_data = avlb_data[["trade_date", "instrument", "sectorL1"]]
        return avlb_data

    def save(self, new_data: pd.DataFrame, calendar: CCalendar, ):
        test_returns_avlb_dir = self.test_returns_avlb_raw_dir
        db_struct_ret = gen_test_returns_avlb_db(
            test_returns_avlb_dir=test_returns_avlb_dir,
            ret_class=self.ret.ret_class,
            ret=self.ret,
        )
        check_and_makedirs(db_struct_ret.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_ret.db_save_dir,
            db_name=db_struct_ret.db_name,
            table=db_struct_ret.table,
            mode="a",
        )
        if sqldb.check_continuity(new_data["trade_date"].iloc[0], calendar) == 0:
            instru_tst_ret_agg_data = new_data[db_struct_ret.table.vars.names]
            sqldb.update(update_data=instru_tst_ret_agg_data)
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        logger.info(f"Calculate available test return ret = {SFG(self.ret.ret_name)}")
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        base_bgn_date = calendar.get_next_date(iter_dates[0], -self.ret.shift)
        base_end_date = calendar.get_next_date(iter_dates[-1], -self.ret.shift)
        base_stp_date = calendar.get_next_date(base_end_date, shift=1)

        # avlb raw
        ref_tst_ret_data = self.load_ref_ret(base_bgn_date, base_stp_date)
        available_data = self.load_available(base_bgn_date, base_stp_date)
        tst_ret_avlb_data = pd.merge(
            left=available_data,
            right=ref_tst_ret_data,
            on=["trade_date", "instrument"],
            how="left",
        ).sort_values(by=["trade_date", "sectorL1"])
        tst_ret_avlb_raw_data = tst_ret_avlb_data.query(
            f"trade_date >= '{base_bgn_date}' & trade_date <= '{base_stp_date}'")
        self.save(tst_ret_avlb_raw_data, calendar)

        return 0


class CTestReturnLoader(CTestReturnLoaderBase):
    def __init__(self, ret: CRet, test_returns_avlb_dir: str):
        """

        :param ret:
        :param test_returns_avlb_dir: test_returns_avlb_raw_dir or test_returns_avlb_neu_dir
        """
        self.ret = ret
        self.test_returns_avlb_dir = test_returns_avlb_dir

    @property
    def shift(self) -> int:
        return self.ret.shift

    @property
    def ret_name(self) -> str:
        return self.ret.ret_name

    @property
    def value_columns(self) -> list[str]:
        return ["trade_date", "instrument", self.ret.ret_name]

    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        db_struct_ret = gen_test_returns_avlb_db(
            test_returns_avlb_dir=self.test_returns_avlb_dir,
            ret_class=self.ret.ret_class,
            ret=self.ret,
        )
        check_and_makedirs(db_struct_ret.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_ret.db_save_dir,
            db_name=db_struct_ret.db_name,
            table=db_struct_ret.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date, value_columns=self.value_columns)
        return data
