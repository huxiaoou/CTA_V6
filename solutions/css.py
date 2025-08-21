import pandas as pd
from loguru import logger
from husfort.qutility import check_and_makedirs, SFG
from husfort.qsqlite import CMgrSqlDb, CDbStruct
from husfort.qcalendar import CCalendar
from math_tools.weighted import weighted_volatility


class CCrossSectionCalculator:
    def __init__(self, db_struct_avlb: CDbStruct, db_struct_css: CDbStruct):
        self.db_struct_avlb = db_struct_avlb
        self.db_struct_css = db_struct_css

    def load_avlb_data(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct_avlb.db_save_dir,
            db_name=self.db_struct_avlb.db_name,
            table=self.db_struct_avlb.table,
            mode="r",
        )
        avlb_data = sqldb.read_by_range(bgn_date=bgn_date, stp_date=stp_date)
        return avlb_data

    @staticmethod
    def cal_cs_vol(z: pd.DataFrame, ret: str = "return", amt: str = "amount") -> pd.Series:
        """

        :param z: columns contains [ret, amt] at least
        :param ret:
        :param amt:
        :return:
        """
        wgt = z[amt] / z[amt].sum()
        d = {
            "volatility": weighted_volatility(x=z[ret], wgt=wgt),
            "skewness": z[ret].skew(),
            "kurtosis": z[ret].kurtosis(),
        }
        return pd.Series(d)

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
        if sqldb.check_continuity(bgn_date, calendar) == 0:
            sqldb.update(update_data=new_data)

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        buffer_bgn_date = calendar.get_next_date(bgn_date, shift=-5)
        avlb_data = self.load_avlb_data(buffer_bgn_date, stp_date)
        css = avlb_data.groupby(by="trade_date").apply(self.cal_cs_vol)
        new_data = css.reset_index()
        new_data["vma"] = new_data["volatility"].rolling(window=5).mean()
        new_data["sma"] = new_data["skewness"].rolling(window=5).mean()
        new_data["kma"] = new_data["kurtosis"].rolling(window=5).mean()
        new_data["tot_wgt"] = new_data["vma"].map(lambda z: 1 if z < 0.0175 else 0.5)
        new_data = new_data.query(f"trade_date >= '{bgn_date}'")
        self.save(new_data=new_data, bgn_date=bgn_date, calendar=calendar)
        logger.info(f"{SFG('Cross section stats')} calculated.")
        return 0
