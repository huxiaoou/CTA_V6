import os
from itertools import product
from dataclasses import dataclass
from husfort.qsqlite import CDbStruct
from typedefs.typedefInstrus import TUniverse
from typedefs.typedefReturns import TReturnClass, CRet, TRets
from typedefs.typedefStrategies import CStrategy, CPortfolio


@dataclass(frozen=True)
class CCfgMktIdx:
    equity: str
    commodity: str

    @property
    def idxes(self) -> list[str]:
        return [self.equity, self.commodity]


@dataclass(frozen=True)
class CCfgAvlbUnvrs:
    win: int
    amount_threshold: float
    win_vol: int
    win_vol_min: int

    @property
    def buffer_win(self) -> int:
        return max(self.win, self.win_vol, self.win_vol_min)

    @property
    def wins_volatility(self) -> tuple[int, int]:
        return self.win_vol, self.win_vol_min


@dataclass(frozen=True)
class CCfgTst:
    wins: list[int]
    wins_qtest: list[int]  # for ic and vt


@dataclass(frozen=True)
class CCfgConst:
    INIT_CASH: float
    COST_RATE: float
    SECTORS: list[str]
    LAG: int


"""
--------------------------------
Part VI: generic and project
--------------------------------
"""


@dataclass(frozen=True)
class CCfgDbStruct:
    # --- shared database
    macro: CDbStruct
    forex: CDbStruct
    fmd: CDbStruct
    position: CDbStruct
    basis: CDbStruct
    stock: CDbStruct
    preprocess: CDbStruct
    minute_bar: CDbStruct


@dataclass(frozen=True)
class CCfgProj:
    # --- shared
    calendar_path: str
    root_dir: str
    db_struct_path: str
    alternative_dir: str
    market_index_path: str
    by_instru_pos_dir: str
    by_instru_pre_dir: str
    by_instru_min_dir: str
    instru_info_path: str

    # --- project
    project_root_dir: str

    # --- project parameters
    universe: TUniverse
    avlb_unvrs: CCfgAvlbUnvrs
    mkt_idxes: CCfgMktIdx
    const: CCfgConst
    tst: CCfgTst
    strategies: list[CStrategy]
    portfolios: list[CPortfolio]

    @property
    def all_rets(self) -> TRets:
        return [CRet(ret_class=TReturnClass(rc), win=w, lag=self.const.LAG)
                for rc, w in product(TReturnClass, self.tst.wins)]

    @property
    def qtest_rets(self) -> TRets:
        return [CRet(ret_class=TReturnClass(rc), win=w, lag=self.const.LAG)
                for rc, w in product(TReturnClass, self.tst.wins_qtest)]

    @property
    def available_dir(self) -> str:
        return os.path.join(self.project_root_dir, "available")

    @property
    def cross_section_stats_dir(self) -> str:
        return os.path.join(self.project_root_dir, "cross_section_stats")

    @property
    def market_dir(self):
        return os.path.join(self.project_root_dir, "market")

    @property
    def test_returns_by_instru_dir(self):
        return os.path.join(self.project_root_dir, "test_returns_by_instru")

    @property
    def test_returns_avlb_raw_dir(self):
        return os.path.join(self.project_root_dir, "test_returns_avlb_raw")

    @property
    def factors_by_instru_dir(self):
        return os.path.join(self.project_root_dir, "factors_by_instru")

    @property
    def factors_avlb_raw_dir(self):
        return os.path.join(self.project_root_dir, "factors_avlb_raw")

    @property
    def factors_avlb_ewa_dir(self):  # ewa: exponential weighted average
        return os.path.join(self.project_root_dir, "factors_avlb_ewa")

    @property
    def ic_tests_dir(self):
        return os.path.join(self.project_root_dir, "ic_tests")

    @property
    def vt_tests_dir(self):
        return os.path.join(self.project_root_dir, "vt_tests")

    @property
    def optimized_dir(self):
        return os.path.join(self.project_root_dir, "optimized")

    @property
    def signals_factors_dir(self):
        return os.path.join(self.project_root_dir, "signals_factors")

    @property
    def signals_strategies_dir(self):
        return os.path.join(self.project_root_dir, "signals_strategies")

    @property
    def signals_portfolios_dir(self):
        return os.path.join(self.project_root_dir, "signals_portfolios")

    @property
    def simulations_dir(self):
        return os.path.join(self.project_root_dir, "simulations")

    @property
    def evaluations_dir(self):
        return os.path.join(self.project_root_dir, "evaluations")

    @property
    def sims_quick_dir(self):
        return os.path.join(self.project_root_dir, "sims_quick")

    @property
    def factors_corr_dir(self):
        return os.path.join(self.project_root_dir, "factors_corr")


TFactorsAvlbDirType = str
TTestReturnsAvlbDirType = str
