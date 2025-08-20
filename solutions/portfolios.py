import numpy as np
import pandas as pd
from husfort.qutility import check_and_makedirs, SFG
from husfort.qsqlite import CMgrSqlDb
from husfort.qcalendar import CCalendar
from husfort.qlog import logger
from typedefs.typedefStrategies import CPortfolio
from husfort.qsimulation import gen_nav_db


class CSimPortfolio:
    def __init__(self, portfolio: CPortfolio, simulations_dir: str):
        self.portfolio = portfolio
        self.simulations_dir = simulations_dir

    def load_strategy_ret(self, strategy_name: str, bgn_date: str, stp_date: str) -> pd.Series:
        db_struct = gen_nav_db(save_dir=self.simulations_dir, save_id=strategy_name)
        db = CMgrSqlDb(
            db_save_dir=db_struct.db_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="r",
        )
        data = db.read_by_range(
            bgn_date, stp_date, value_columns=["trade_date", "ret"],
        ).set_index("trade_date")["ret"]
        return data

    def load_strategies_ret(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        ret_data = {}
        for strategy_name in self.portfolio.strategies_weights:
            ret_data[strategy_name] = self.load_strategy_ret(
                strategy_name=strategy_name,
                bgn_date=bgn_date,
                stp_date=stp_date,
            )
        ret_data = pd.DataFrame(ret_data)
        return ret_data

    def save(self, nav_data: pd.DataFrame, calendar: CCalendar):
        check_and_makedirs(self.simulations_dir)
        db_struct = gen_nav_db(self.simulations_dir, save_id=self.portfolio.name)
        sqldb = CMgrSqlDb(
            db_save_dir=db_struct.db_save_dir,
            db_name=db_struct.db_name,
            table=db_struct.table,
            mode="a",
        )
        if sqldb.check_continuity(incoming_date=nav_data["trade_date"].iloc[0], calendar=calendar) == 0:
            sqldb.update(update_data=nav_data)

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        ret = self.load_strategies_ret(bgn_date, stp_date)
        wgt = self.portfolio.weight
        portfolio_ret = ret @ wgt
        portfolio_navps = (1 + portfolio_ret).cumprod()
        portfolio_nav = portfolio_navps
        portfolio_data = pd.DataFrame({
            "init_cash": np.nan,
            "tot_realized_pnl": np.nan,
            "this_day_realized_pnl": np.nan,
            "this_day_cost": np.nan,
            "tot_unrealized_pnl": np.nan,
            "last_nav": np.nan,
            "nav": portfolio_nav,
            "navps": portfolio_navps,
            "ret": portfolio_ret,
        })
        portfolio_data = portfolio_data.reset_index()
        self.save(nav_data=portfolio_data, calendar=calendar)
        return 0


def main_sims_portfolios(
        portfolios: list[CPortfolio],
        simulations_dir: str,
        bgn_date: str,
        stp_date: str,
        calendar: CCalendar,
):
    for portfolio in portfolios:
        p = CSimPortfolio(portfolio, simulations_dir)
        p.main(bgn_date=bgn_date, stp_date=stp_date, calendar=calendar)
        logger.info(f"Portfolio {SFG(portfolio.name)} is generated.")
    return 0
