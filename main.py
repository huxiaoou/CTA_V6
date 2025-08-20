import argparse

from solutions.factor import CCfgFactors


def parse_args(cfg_facs: CCfgFactors):
    arg_parser = argparse.ArgumentParser(description="To calculate data, such as macro and forex")
    arg_parser.add_argument("--bgn", type=str, help="begin date, format = [YYYYMMDD]", required=True)
    arg_parser.add_argument("--stp", type=str, help="stop  date, format = [YYYYMMDD]")
    arg_parser.add_argument("--nomp", default=False, action="store_true",
                            help="not using multiprocess, for debug. Works only when switch in ('factor', 'signals', 'simulations', 'quick')")
    arg_parser.add_argument("--processes", type=int, default=None,
                            help="number of processes to be called, effective only when nomp = False")
    arg_parser.add_argument("--verbose", default=False, action="store_true",
                            help="whether to print more details, effective only when sub function = (feature_selection,)")

    arg_parser_subs = arg_parser.add_subparsers(
        title="Position argument to call sub functions",
        dest="switch",
        description="use this position argument to call different functions of this project. "
                    "For example: 'python main.py --bgn 20120104 --stp 20240826 available'",
        required=True,
    )

    # switch: available
    arg_parser_subs.add_parser(name="available", help="Calculate available universe")

    # switch: available
    arg_parser_subs.add_parser(name="css", help="Calculate cross section statistics")

    # switch: market
    arg_parser_subs.add_parser(name="market", help="Calculate market universe")

    # switch: test return
    arg_parser_subs.add_parser(name="test_return", help="Calculate test returns")

    # switch: factor
    arg_parser_sub = arg_parser_subs.add_parser(name="factor", help="Calculate factor")
    arg_parser_sub.add_argument(
        "--fclass", type=str,
        help="factor class to run",
        required=True, choices=cfg_facs.classes,
    )

    # switch: ic
    arg_parser_sub = arg_parser_subs.add_parser(name="ic", help="Calculate ic_tests")
    arg_parser_sub.add_argument(
        "--fclass", type=str,
        help="factor class to test",
        required=True, choices=cfg_facs.classes,
    )
    arg_parser_sub.add_argument(
        "--va", default=False, action="store_true",
        help="using volatility to adjust",
    )

    # switch: vt
    arg_parser_sub = arg_parser_subs.add_parser(name="vt", help="Calculate vt_tests")
    arg_parser_sub.add_argument(
        "--fclass", type=str,
        help="factor class to test",
        required=True, choices=cfg_facs.classes,
    )
    arg_parser_sub.add_argument(
        "--va", default=False, action="store_true",
        help="using volatility to adjust",
    )

    # switch: test return
    arg_parser_subs.add_parser(name="optimize", help="Calculate optimal weights of factors in strategies")

    # switch: signals
    arg_parser_sub = arg_parser_subs.add_parser(
        name="signals", help="Calculate signals for factors or strategies.")
    arg_parser_sub.add_argument(
        "--type", type=str, choices=("factors", "strategies"),
        help="options for --type:('factors', 'strategies')",
        required=True,
    )

    # switch: simulations
    arg_parser_subs.add_parser(name="simulations", help="Calculate simulations for strategies and portfolios.")

    # switch: evaluations
    arg_parser_subs.add_parser(name="quick", help="Calculate quick simulations for signals")

    # switch: fcorr
    arg_parser_sub = arg_parser_subs.add_parser(name="fcorr", help="Calculate correlations between 2 factors")
    arg_parser_sub.add_argument("--f0", type=str, required=True, help="first factor name, like 'MTM240'")
    arg_parser_sub.add_argument("--f1", type=str, required=True, help="Second factor name, like 'TS240'")

    # switch: test
    arg_parser_subs.add_parser(name="test", help="Test some functions")
    return arg_parser.parse_args()


if __name__ == "__main__":
    from loguru import logger
    from config import proj_cfg, db_struct_cfg, cfg_factors
    from husfort.qlog import define_logger
    from husfort.qcalendar import CCalendar
    from solutions.shared import get_avlb_db, get_market_db, get_css_db

    define_logger()

    calendar = CCalendar(proj_cfg.calendar_path)
    args = parse_args(cfg_facs=cfg_factors)
    bgn_date, stp_date = args.bgn, args.stp or calendar.get_next_date(args.bgn, shift=1)
    db_struct_avlb = get_avlb_db(proj_cfg.available_dir)
    db_struct_mkt = get_market_db(proj_cfg.market_dir, proj_cfg.const.SECTORS)
    db_struct_css = get_css_db(proj_cfg.cross_section_stats_dir)

    if args.switch == "available":
        from solutions.available import main_available

        main_available(
            bgn_date=bgn_date, stp_date=stp_date,
            universe=proj_cfg.universe,
            cfg_avlb_unvrs=proj_cfg.avlb_unvrs,
            db_struct_preprocess=db_struct_cfg.preprocess,
            db_struct_avlb=db_struct_avlb,
            calendar=calendar,
        )
    elif args.switch == "css":
        from solutions.css import CCrossSectionCalculator

        css = CCrossSectionCalculator(
            db_struct_avlb=db_struct_avlb,
            db_struct_css=db_struct_css,
        )
        css.main(bgn_date=bgn_date, stp_date=stp_date, calendar=calendar)
    elif args.switch == "market":
        from solutions.market import main_market

        main_market(
            bgn_date=bgn_date, stp_date=stp_date,
            calendar=calendar,
            db_struct_avlb=db_struct_avlb,
            db_struct_mkt=db_struct_mkt,
            path_mkt_idx_data=proj_cfg.market_index_path,
            mkt_idxes=proj_cfg.mkt_idxes.idxes,
            sectors=proj_cfg.const.SECTORS,
        )
    elif args.switch == "test_return":
        from solutions.test_return import CTestReturnsByInstru, CTestReturnsAvlb

        for ret in proj_cfg.all_rets:
            test_returns_by_instru = CTestReturnsByInstru(
                ret=ret, universe=proj_cfg.universe,
                test_returns_by_instru_dir=proj_cfg.test_returns_by_instru_dir,
                db_struct_preprocess=db_struct_cfg.preprocess,
            )
            test_returns_by_instru.main(bgn_date, stp_date, calendar)
            test_returns_avlb = CTestReturnsAvlb(
                ret=ret, universe=proj_cfg.universe,
                test_returns_by_instru_dir=proj_cfg.test_returns_by_instru_dir,
                test_returns_avlb_raw_dir=proj_cfg.test_returns_avlb_raw_dir,
                db_struct_avlb=db_struct_avlb,
            )
            test_returns_avlb.main(bgn_date, stp_date, calendar)
    elif args.switch == "factor":
        from solutions.factor import CFactorsAvlb, pick_factor
        from husfort.qinstruments import CInstruMgr

        instru_mgr = CInstruMgr(instru_info_path=proj_cfg.instru_info_path, key="tushareId")
        cfg, fac = pick_factor(
            fclass=args.fclass,
            cfg_factors=cfg_factors,
            factors_by_instru_dir=proj_cfg.factors_by_instru_dir,
            universe=proj_cfg.universe,
            preprocess=db_struct_cfg.preprocess,
            minute_bar=db_struct_cfg.minute_bar,
            db_struct_pos=db_struct_cfg.position,
            db_struct_forex=db_struct_cfg.forex,
            db_struct_macro=db_struct_cfg.macro,
            db_struct_mkt=db_struct_mkt,
            instru_mgr=instru_mgr,
        )
        fac.main(
            bgn_date=bgn_date, stp_date=stp_date, calendar=calendar,
            call_multiprocess=not args.nomp, processes=args.processes,
        )
        fac_avlb = CFactorsAvlb(
            factor_grp=cfg,
            universe=proj_cfg.universe,
            factors_by_instru_dir=proj_cfg.factors_by_instru_dir,
            factors_avlb_raw_dir=proj_cfg.factors_avlb_raw_dir,
            factors_avlb_ewa_dir=proj_cfg.factors_avlb_ewa_dir,
            db_struct_avlb=db_struct_avlb,
        )
        fac_avlb.main(bgn_date, stp_date, calendar)
    elif args.switch in ("ic", "vt"):
        from solutions.qtests import main_qtests, TICTestAuxArgs

        factor_grp = cfg_factors.get_cfg(factor_class=args.fclass)
        aux_args_list: list[TICTestAuxArgs] = [
            (proj_cfg.factors_avlb_ewa_dir, proj_cfg.test_returns_avlb_raw_dir)
        ]
        main_qtests(
            rets=proj_cfg.qtest_rets,
            factor_grp=factor_grp,
            aux_args_list=aux_args_list,
            tests_dir=proj_cfg.ic_tests_dir if args.switch == "ic" else proj_cfg.vt_tests_dir,
            db_struct_avlb=db_struct_avlb,
            bgn_date=bgn_date,
            stp_date=stp_date,
            calendar=calendar,
            test_type=args.switch,
            volatility_adjusted=args.va,
        )
    elif args.switch == "signals":
        from solutions.signals import main_signals

        if args.type == "factors":
            from solutions.signals import gen_signals_from_factors

            desc = "Calculate signals from factors"
            signals = gen_signals_from_factors(
                factor_cfgs=cfg_factors.get_cfgs(),
                factors_avlb_dir=proj_cfg.factors_avlb_ewa_dir,
                signals_factors_dir=proj_cfg.signals_factors_dir,
            )

        elif args.type == "strategies":
            from solutions.signals import gen_signals_from_strategies

            desc = "Calculate signals from strategies"
            signals = gen_signals_from_strategies(
                strategies=proj_cfg.strategies,
                signals_strategies_dir=proj_cfg.signals_strategies_dir,
                signals_factors_dir=proj_cfg.signals_factors_dir,
                optimize_dir=proj_cfg.optimize_dir,
            )
        else:
            raise ValueError(f"Invalid argument 'type' value: {args.type}")

        main_signals(
            signals=signals,
            bgn_date=bgn_date,
            stp_date=stp_date,
            calendar=calendar,
            call_multiprocess=not args.nomp,
            processes=args.processes,
            desc=desc,
        )

    elif args.switch == "optimize":
        from solutions.optimize import main_optimize

        main_optimize(
            strategies=proj_cfg.strategies,
            bgn_date=bgn_date,
            stp_date=stp_date,
            calendar=calendar,
            method="VT",
            optimize_dir=proj_cfg.optimize_dir,
            vt_tests_dir=proj_cfg.vt_tests_dir,
        )

    elif args.switch == "simulations":
        from solutions.simulations import main_sims
        from solutions.evaluations import main_evl_strategies_and_portfolios
        from solutions.portfolios import main_sims_portfolios

        main_sims(
            strategies=proj_cfg.strategies,
            signals_strategies_dir=proj_cfg.signals_strategies_dir,
            init_cash=proj_cfg.const.INIT_CASH,
            cost_rate=proj_cfg.const.COST_RATE,
            instru_info_path=proj_cfg.instru_info_path,
            universe=list(proj_cfg.universe),
            preprocess=db_struct_cfg.preprocess,
            fmd=db_struct_cfg.fmd,
            bgn_date=bgn_date,
            stp_date=stp_date,
            calendar=calendar,
            sim_save_dir=proj_cfg.simulations_dir,
            call_multiprocess=not args.nomp,
            processes=args.processes,
            verbose=args.verbose,
        )

        main_sims_portfolios(
            portfolios=proj_cfg.portfolios,
            simulations_dir=proj_cfg.simulations_dir,
            bgn_date=bgn_date,
            stp_date=stp_date,
            calendar=calendar,
        )

        main_evl_strategies_and_portfolios(
            strategies=proj_cfg.strategies,
            portfolios=proj_cfg.portfolios,
            sim_save_dir=proj_cfg.simulations_dir,
            evl_save_dir=proj_cfg.evaluations_dir,
        )
    elif args.switch == "quick":
        from solutions.sims_quick import main_sims_quick

        main_sims_quick(
            strategies=proj_cfg.strategies,
            signals_strategies_dir=proj_cfg.signals_strategies_dir,
            test_returns_avlb_raw_dir=proj_cfg.test_returns_avlb_raw_dir,
            cost_rate=proj_cfg.const.COST_RATE,
            sims_quick_dir=proj_cfg.sims_quick_dir,
            bgn_date=bgn_date,
            stp_date=stp_date,
            calendar=calendar,
            call_multiprocess=not args.nomp,
            processes=args.processes,
        )
    elif args.switch == "fcorr":
        from solutions.factor import cal_corr_2f

        f0, f1 = cfg_factors.match_factor(args.f0), cfg_factors.match_factor(args.f1)
        cal_corr_2f(
            f0=f0, f1=f1, factors_avlb_dir=proj_cfg.factors_avlb_raw_dir,
            bgn_date=bgn_date, stp_date=stp_date,
            factors_corr_dir=proj_cfg.factors_corr_dir,
        )
        cal_corr_2f(
            f0=f0, f1=f1, factors_avlb_dir=proj_cfg.factors_avlb_ewa_dir,
            bgn_date=bgn_date, stp_date=stp_date,
            factors_corr_dir=proj_cfg.factors_corr_dir,
        )

    elif args.switch == "test":
        logger.info("Do some tests")
