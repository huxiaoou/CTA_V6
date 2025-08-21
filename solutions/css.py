import pandas as pd
from loguru import logger
from husfort.qutility import check_and_makedirs, SFG
from husfort.qsqlite import CMgrSqlDb, CDbStruct
from husfort.qcalendar import CCalendar
from math_tools.weighted import weighted_volatility


class CCrossSectionCalculator:
    def __init__(
            self,
            db_struct_avlb: CDbStruct,
            db_struct_mkt: CDbStruct,
            db_struct_css: CDbStruct,
            sectors: list[str],
    ):
        self.db_struct_avlb = db_struct_avlb
        self.db_struct_mkt = db_struct_mkt
        self.db_struct_css = db_struct_css
        self.sectors = sectors

    def load_avlb_data(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_avlb.db_save_dir,
            db_name=self.db_struct_avlb.db_name,
            table=self.db_struct_avlb.table,
            mode="r",
        )
        avlb_data = sqldb.read_by_range(bgn_date=bgn_date, stp_date=stp_date)
        return avlb_data

    def load_mkt_idx(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_mkt.db_save_dir,
            db_name=self.db_struct_mkt.db_name,
            table=self.db_struct_mkt.table,
            mode="r",
        )
        mkt_idx_data = sqldb.read_by_range(bgn_date=bgn_date, stp_date=stp_date)
        return mkt_idx_data

    @staticmethod
    def cal_cs_vol(data: pd.DataFrame, ret: str = "return", amt: str = "amount") -> pd.Series:
        """

        :param data: columns contains [ret, amt] at least
        :param ret:
        :param amt:
        :return:
        """
        d = {
            "volatility": weighted_volatility(x=data[ret], wgt=data[amt]),
            "skewness": data[ret].skew(),
            "kurtosis": data[ret].kurtosis(),
        }
        sector_volatility = data.groupby(by="sectorL1").apply(
            lambda _: weighted_volatility(x=_[ret], wgt=_[amt])
        )
        return pd.concat([pd.Series(d), sector_volatility], axis=0)

    def save(self, new_data: pd.DataFrame, bgn_date: str, calendar: CCalendar):
        """

        :param new_data: "trade_date" as the first column
        :param bgn_date:
        :param calendar:
        :return:
        """
        check_and_makedirs(self.db_struct_css.db_save_dir)
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_css.db_save_dir,
            db_name=self.db_struct_css.db_name,
            table=self.db_struct_css.table,
            mode="a",
        )
        save_data = new_data[self.db_struct_css.table.vars.names]
        if sqldb.check_continuity(bgn_date, calendar) == 0:
            sqldb.update(update_data=save_data)

    @property
    def rename_mapper(self) -> dict:
        return {s: f"volatility_{s}" for s in self.sectors}

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        buffer_bgn_date = calendar.get_next_date(bgn_date, shift=-5)
        mkt_idx_data = self.load_mkt_idx(buffer_bgn_date, stp_date)
        mkt_idx_data["volatility_sector"] = mkt_idx_data[self.sectors].std(axis=1).rolling(window=5).mean()
        avlb_data = self.load_avlb_data(buffer_bgn_date, stp_date)
        css = avlb_data.groupby(by="trade_date").apply(self.cal_cs_vol)
        css[self.sectors] = css[self.sectors].rolling(window=5).mean()
        new_data = css.reset_index().rename(columns=self.rename_mapper)
        new_data["vma"] = new_data["volatility"].rolling(window=5).mean()
        new_data["sma"] = new_data["skewness"].rolling(window=5).mean()
        new_data["kma"] = new_data["kurtosis"].rolling(window=5).mean()
        new_data["tot_wgt"] = new_data["vma"].map(lambda z: 1 if z < 0.0175 else 0.5)
        new_data = new_data.merge(
            right=mkt_idx_data[["trade_date", "volatility_sector"]],
            on="trade_date", how="left",
        )
        new_data = new_data.query(f"trade_date >= '{bgn_date}'")
        self.save(new_data=new_data, bgn_date=bgn_date, calendar=calendar)
        logger.info(f"{SFG('Cross section stats')} calculated.")
        return 0
