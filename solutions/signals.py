import pandas as pd
import multiprocessing as mp
from rich.progress import Progress
from husfort.qsqlite import CMgrSqlDb, CDbStruct
from husfort.qcalendar import CCalendar
from husfort.qutility import check_and_makedirs, error_handler
from husfort.qsimquick import CSignalsLoaderBase
from typedefs.typedefFactors import CCfgFactorGrp
from solutions.factor import CFactorsLoader
from solutions.shared import gen_sig_fac_db
from math_tools.weighted import map_to_weight


class CSignals(CSignalsLoaderBase):
    def __init__(self, signals_dir: str, signal_id: str):
        self.signals_dir = signals_dir
        self._signal_id = signal_id

    @property
    def signal_id(self):
        return self._signal_id

    def get_sig_db_struct(self) -> CDbStruct:
        raise NotImplementedError

    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        db_struct = self.get_sig_db_struct()
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct.db_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date)
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


class CSignalsFactors(CSignals):
    def __init__(self, factor_grp: CCfgFactorGrp, factors_avlb_dir: str, signals_dir: str):
        super().__init__(signals_dir, signal_id=factor_grp.factor_class)
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
        weight_data = grp_data[self.factor_grp.factor_names].apply(map_to_weight)
        save_data = pd.merge(
            left=factor_data[["trade_date", "instrument"]],
            right=weight_data,
            left_index=True,
            right_index=True,
            how="left",
        )
        self.save(save_data, calendar)
        return 0


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
        for s in signals:
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
            signals_dir=signals_factors_dir,
        )
        for z in factor_cfgs
    ]
