import os
import pandas as pd
from rich.progress import track
from husfort.qevaluation import CNAV
from husfort.qsqlite import CMgrSqlDb
from husfort.qsimulation import gen_nav_db
from husfort.qutility import check_and_makedirs
from husfort.qplot import CPlotLines
from typedefs.typedefStrategies import CStrategy


def evl_sim(sim_id: str, sim_save_dir: str, evl_save_dir: str) -> dict:
    db_struct = gen_nav_db(save_dir=sim_save_dir, save_id=sim_id)
    sqldb = CMgrSqlDb(
        db_save_dir=db_struct.db_save_dir,
        db_name=db_struct.db_name,
        table=db_struct.table,
        mode="r",
    )
    ret_data = sqldb.read(value_columns=["trade_date", "ret"]).set_index("trade_date")

    # by year
    ret_data["trade_year"] = ret_data.index.map(lambda z: z[0:4])
    summary_by_year = {}
    for trade_year, trade_year_data in ret_data.groupby("trade_year"):
        nav_y = CNAV(input_srs=trade_year_data["ret"], input_type="RET")
        nav_y.cal_all_indicators(excluded=("var", "ldd", "lrd"))
        summary_by_year[trade_year] = nav_y.reformat_to_display()
    summary_by_year = pd.DataFrame.from_dict(summary_by_year, orient="index")
    summary_by_id_rpt_dir = os.path.join(evl_save_dir, "by_id_rpt")
    check_and_makedirs(summary_by_id_rpt_dir)
    summary_save_file = f"{sim_id}.csv"
    summary_save_path = os.path.join(summary_by_id_rpt_dir, summary_save_file)
    summary_by_year.to_csv(summary_save_path, float_format="%.4f", index_label="trade_year")

    # all plot
    summary_by_id_plt_dir = os.path.join(evl_save_dir, "by_id_plt")
    check_and_makedirs(summary_by_id_plt_dir)
    ret_data["nav"] = (ret_data["ret"] + 1).cumprod()
    artist = CPlotLines(
        plot_data=ret_data[["nav"]],
        fig_name=f"{sim_id}",
        fig_save_dir=summary_by_id_plt_dir,
    )
    artist.plot()
    artist.set_axis_x(xtick_count=12)
    artist.save_and_close()

    # all sum
    nav = CNAV(input_srs=ret_data["ret"], input_type="RET")
    nav.cal_all_indicators(excluded=("var", "ldd", "lrd"))
    d = nav.reformat_to_display()

    return d


def main_evl_strategies(strategies: list[CStrategy], sim_save_dir: str, evl_save_dir: str):
    summary_all = []
    for strategy in track(strategies, description="Evaluating strategies"):
        d = evl_sim(sim_id=strategy.name, sim_save_dir=sim_save_dir, evl_save_dir=evl_save_dir)
        args_data = {
            "ret": strategy.ret.ret_name,
        }
        d.update(args_data)
        summary_all.append(d)
    summary_all = pd.DataFrame(summary_all)
    summary_all["score"] = summary_all["score"].map(lambda _: float(_))
    summary_all.sort_values(by="score", ascending=False, inplace=True)
    summary_all_file = "summary_all.csv"
    summary_all_path = os.path.join(evl_save_dir, summary_all_file)
    summary_all.to_csv(summary_all_path, float_format="%.3f", index=False)

    score, sharpe, calmar = summary_all["score"].iloc[0], summary_all["sharpe"].iloc[0], summary_all["calmar"].iloc[0]
    print(f"Best score = {score:>6.3f}. (Sharpe, Calmar) = ({sharpe}, {calmar}).")
    print(summary_all.head(12))
    return 0
