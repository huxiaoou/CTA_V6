import numpy as np
import pandas as pd
from husfort.qutility import check_and_makedirs, SFG
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CDbStruct, CMgrSqlDb
from husfort.qlog import logger
from typedefs.typedefInstrus import TUniverse
from typedef import CCfgICov
from solutions.shared import get_icov_db


class CICOVReader:
    def __init__(self, icov_db_dir: str):
        self.db_struct_icov = get_icov_db(icov_db_dir)

    def read(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_icov.db_save_dir,
            db_name=self.db_struct_icov.db_name,
            table=self.db_struct_icov.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date)
        return data


class CICOV(CICOVReader):
    def __init__(
            self,
            cfg_icov: CCfgICov,
            universe: TUniverse,
            db_struct_preprocess: CDbStruct,
            icov_db_dir: str,
    ):
        super().__init__(icov_db_dir=icov_db_dir)
        self.cfg_icov = cfg_icov
        self.universe = universe
        self.db_struct_preprocess = db_struct_preprocess

    def load_rets_by_instru(self, instru: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
        db_struct_instru = self.db_struct_preprocess.copy_to_another(another_db_name=f"{instru}.db")
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct_instru.db_save_dir,
            db_name=db_struct_instru.db_name,
            table=db_struct_instru.table,
            mode="r",
        )
        amt_data = sqldb.read_by_range(bgn_date, stp_date, value_columns=["trade_date", "return_c_major"])
        amt_data = amt_data.rename(columns={"return_c_major": instru}).set_index("trade_date")
        return amt_data

    def load_rets(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        instru_data: list[pd.DataFrame] = []
        for instru in self.universe:
            instru_data.append(self.load_rets_by_instru(instru, bgn_date, stp_date))
        rets = pd.concat(instru_data, axis=1, ignore_index=False).fillna(0)
        return rets

    @staticmethod
    def reformat(icov_square: pd.DataFrame, bgn_date: str) -> pd.DataFrame:
        icov_duplicated = icov_square.fillna(0).stack().reset_index().rename(
            columns={
                "level_0": "trade_date",
                "level_1": "i0",
                "level_2": "i1",
                0: "icov",
            }
        )
        icov = icov_duplicated.query(f"i0 <= i1 and trade_date >= '{bgn_date}'")
        icov = icov.sort_values(["trade_date", "i0", "i1"], ascending=True)
        return icov

    def save(self, icov: pd.DataFrame, bgn_date: str, calendar: CCalendar):
        check_and_makedirs(self.db_struct_icov.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_icov.db_save_dir,
            db_name=self.db_struct_icov.db_name,
            table=self.db_struct_icov.table,
            mode="a",
        )
        if sqldb.check_continuity(bgn_date, calendar) == 0:
            sqldb.update(update_data=icov)
        return 0

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        buffer_bgn_date = calendar.get_next_date(bgn_date, shift=-self.cfg_icov.win + 1)
        rets = self.load_rets(buffer_bgn_date, stp_date)
        icov_square = rets.rolling(self.cfg_icov.win).cov() * 1e4
        icov = self.reformat(icov_square, bgn_date=bgn_date)
        self.save(icov, bgn_date, calendar)
        logger.info(f"instruments covariance from {SFG(bgn_date)} to {SFG(stp_date)} calculated")
        return 0


def get_cov_at_trade_date(icov_data: pd.DataFrame, trade_date: str, instruments: list[str]) -> pd.DataFrame:
    trade_date_icov = icov_data.query(f"trade_date == '{trade_date}'")
    partial_cov = trade_date_icov.pivot(
        index="instrument0", columns="instrument1", values="cov",
    ).fillna(0).loc[instruments, instruments]
    variance = pd.DataFrame(data=np.diag(np.diag(partial_cov)), index=partial_cov.index,
                            columns=partial_cov.columns)
    instrus_cov = partial_cov + partial_cov.T - variance
    return instrus_cov
