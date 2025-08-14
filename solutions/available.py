import pandas as pd
from husfort.qutility import check_and_makedirs, qtimer
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CDbStruct, CMgrSqlDb
from typedefs.typedefInstrus import TUniverse
from typedef import CCfgAvlbUnvrs


def load_major(db_struct_instru: CDbStruct, bgn_date: str, stp_date: str) -> pd.DataFrame:
    sqldb = CMgrSqlDb(
        db_save_dir=db_struct_instru.db_save_dir,
        db_name=db_struct_instru.db_name,
        table=db_struct_instru.table,
        mode="r",
    )
    amt_data = sqldb.read_by_range(bgn_date, stp_date, value_columns=["trade_date", "return_c_major", "amount_major"])
    return amt_data


def reformat(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data.rename(
        columns={"return_c_major": "return", "amount_major": "amount"}
    ).set_index("trade_date")


def get_available_universe_by_date(x: pd.Series, ret_df: pd.DataFrame, amt_df: pd.DataFrame) -> pd.DataFrame:
    # x is a Series like this: pd.Series({"cu":True, "CY":False}, name="20120104")
    trade_date: str = x.name  # type:ignore
    sub_available_universe_df = pd.DataFrame(
        {
            "return": ret_df.loc[trade_date, x],  # type:ignore
            "amount": amt_df.loc[trade_date, x],  # type:ignore
        }
    )
    sub_available_universe_df["trade_date"] = trade_date
    return sub_available_universe_df


def get_available_universe(
        bgn_date: str,
        stp_date: str,
        db_struct_preprocess: CDbStruct,
        db_struct_avlb: CDbStruct,
        universe: TUniverse,
        cfg_avlb_unvrs: CCfgAvlbUnvrs,
        calendar: CCalendar,
) -> pd.DataFrame:
    win_start_date = calendar.get_next_date(bgn_date, -cfg_avlb_unvrs.buffer_win + 1)
    win_vol, win_vol_min = cfg_avlb_unvrs.wins_volatility
    amt_data, amt_ma_data, return_data, volatility = {}, {}, {}, {}
    for instru in universe:
        db_struct_instru = db_struct_preprocess.copy_to_another(another_db_name=f"{instru}.db")
        instru_major_data = load_major(db_struct_instru=db_struct_instru, bgn_date=win_start_date, stp_date=stp_date)
        selected_major_data = reformat(instru_major_data)
        amt_ma_data[instru] = selected_major_data["amount"].fillna(0).rolling(window=cfg_avlb_unvrs.win).mean()
        amt_data[instru] = selected_major_data["amount"].fillna(0)
        return_data[instru] = selected_major_data["return"]
        volatility[instru] = selected_major_data["return"].rolling(window=win_vol, min_periods=win_vol_min).std()
    amt_df, amt_ma_df = pd.DataFrame(amt_data), pd.DataFrame(amt_ma_data)
    ret_df, vol_df = pd.DataFrame(return_data), pd.DataFrame(volatility)

    # --- reorganize and save
    filter_df: pd.DataFrame = amt_ma_df.ge(cfg_avlb_unvrs.amount_threshold)
    filter_df = filter_df.truncate(before=bgn_date)
    update_df = pd.DataFrame({
        "return": ret_df[filter_df].stack(),
        "amount": amt_df[filter_df].stack(),
        "volatility": vol_df[filter_df].stack(),
    }).reset_index().rename(columns={"level_1": "instrument"})
    # --- add section
    update_df["sectorL0"] = update_df["instrument"].map(lambda z: universe[z].sectorL0)
    update_df["sectorL1"] = update_df["instrument"].map(lambda z: universe[z].sectorL1)
    update_df = update_df.sort_values(by=["trade_date", "sectorL1"], ascending=True)
    return update_df[db_struct_avlb.table.vars.names]


@qtimer
def main_available(
        bgn_date: str,
        stp_date: str,
        universe: TUniverse,
        cfg_avlb_unvrs: CCfgAvlbUnvrs,
        db_struct_preprocess: CDbStruct,
        db_struct_avlb: CDbStruct,
        calendar: CCalendar,
):
    check_and_makedirs(db_struct_avlb.db_save_dir)
    sqldb = CMgrSqlDb(
        db_save_dir=db_struct_avlb.db_save_dir,
        db_name=db_struct_avlb.db_name,
        table=db_struct_avlb.table,
        mode="a",
    )
    if sqldb.check_continuity(bgn_date, calendar) == 0:
        new_data = get_available_universe(
            bgn_date=bgn_date,
            stp_date=stp_date,
            db_struct_preprocess=db_struct_preprocess,
            db_struct_avlb=db_struct_avlb,
            universe=universe,
            cfg_avlb_unvrs=cfg_avlb_unvrs,
            calendar=calendar,
        )
        print(new_data)
        sqldb.update(update_data=new_data)
    return 0
