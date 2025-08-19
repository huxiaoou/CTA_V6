import multiprocessing as mp
from loguru import logger
from rich.progress import track, Progress
from husfort.qcalendar import CCalendar
from husfort.qinstruments import CInstruMgr
from husfort.qsimulation import CMgrMktData, CMgrMajContract, CSignal, CSimulation
from husfort.qsimulation import TExePriceType
from husfort.qsqlite import CDbStruct
from husfort.qutility import error_handler, qtimer
from solutions.signals import gen_sig_strategy_db
from typedefs.typedefReturns import TReturnClass
from typedefs.typedefStrategies import CStrategy

TSimArgs = tuple[CSignal, TExePriceType]


def covert_strategies_to_sim_args(strategies: list[CStrategy], signals_strategies_dir: str) -> list[TSimArgs]:
    sim_args: list[TSimArgs] = []
    for strategy in strategies:
        signal_db_struct = gen_sig_strategy_db(save_dir=signals_strategies_dir, save_id=strategy.name)
        signal = CSignal(sid=strategy.name, signal_db_struct=signal_db_struct)
        if strategy.ret.ret_class == TReturnClass.OPN:
            sim_args.append((signal, TExePriceType.OPEN))
        else:
            sim_args.append((signal, TExePriceType.CLOSE))
    return sim_args


def process_for_sim(
        signal: CSignal,
        init_cash: float,
        cost_rate: float,
        exe_price_type: TExePriceType,
        mgr_instru: CInstruMgr,
        mgr_maj_contract: CMgrMajContract,
        mgr_mkt_data: CMgrMktData,
        bgn_date: str,
        stp_date: str,
        calendar: CCalendar,
        sim_save_dir: str,
        verbose: bool,
):
    sim = CSimulation(
        signal=signal,
        init_cash=init_cash,
        cost_rate=cost_rate,
        exe_price_type=exe_price_type,
        mgr_instru=mgr_instru,
        mgr_maj_contract=mgr_maj_contract,
        mgr_mkt_data=mgr_mkt_data,
        sim_save_dir=sim_save_dir,
    )
    sim.main(bgn_date=bgn_date, stp_date=stp_date, calendar=calendar, verbose=verbose)
    return 0


@qtimer
def main_sims(
        strategies: list[CStrategy],
        signals_strategies_dir: str,
        init_cash: float,
        cost_rate: float,
        instru_info_path: str,
        universe: list[str],
        preprocess: CDbStruct,
        fmd: CDbStruct,
        bgn_date: str,
        stp_date: str,
        calendar: CCalendar,
        sim_save_dir: str,
        call_multiprocess: bool,
        processes: int,
        verbose: bool,
):
    sim_args = covert_strategies_to_sim_args(strategies, signals_strategies_dir)
    mgr_instru = CInstruMgr(instru_info_path, key="tushareId")
    mgr_maj_contract = CMgrMajContract(universe, preprocess)
    mgr_mkt_data = CMgrMktData(fmd)
    desc = "Do simulations for signals"
    if call_multiprocess:
        logger.info("For simulation, multiprocess is not necessarily faster than uni-process")
        with Progress() as pb:
            main_task = pb.add_task(description=desc, total=len(sim_args))
            with mp.get_context("spawn").Pool(processes=processes) as pool:
                for signal, exe_price_type in sim_args:
                    pool.apply_async(
                        process_for_sim,
                        kwds={
                            "signal": signal,
                            "init_cash": init_cash,
                            "cost_rate": cost_rate,
                            "exe_price_type": exe_price_type,
                            "mgr_instru": mgr_instru,
                            "mgr_maj_contract": mgr_maj_contract,
                            "mgr_mkt_data": mgr_mkt_data,
                            "bgn_date": bgn_date,
                            "stp_date": stp_date,
                            "calendar": calendar,
                            "sim_save_dir": sim_save_dir,
                            "verbose": verbose,
                        },
                        callback=lambda _: pb.update(task_id=main_task, advance=1),
                        error_callback=error_handler,
                    )
                pool.close()
                pool.join()
    else:
        for signal, exe_price_type in track(sim_args, description=desc):
            process_for_sim(
                signal=signal,
                init_cash=init_cash,
                cost_rate=cost_rate,
                exe_price_type=exe_price_type,
                mgr_instru=mgr_instru,
                mgr_maj_contract=mgr_maj_contract,
                mgr_mkt_data=mgr_mkt_data,
                bgn_date=bgn_date,
                stp_date=stp_date,
                calendar=calendar,
                sim_save_dir=sim_save_dir,
                verbose=verbose,
            )
    return 0
