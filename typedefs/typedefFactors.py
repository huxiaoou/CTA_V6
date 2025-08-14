import numpy as np
from dataclasses import dataclass
from itertools import product
from husfort.qcalendar import CCalendar

TFactorClass = str
TFactorName = str
TFactorNames = list[TFactorName]


@dataclass(frozen=True)
class CFactor:
    factor_class: TFactorClass
    factor_name: TFactorName


TFactors = list[CFactor]


@dataclass
class CDecay:
    rate: float
    win: int

    def __post_init__(self):
        rou = np.power(self.rate, 1 / (self.win - 1)) if self.win > 1 else 1.0
        wgt = np.power(rou, np.arange(self.win, 0, -1))
        self.wgt = wgt / wgt.sum()

    def __str__(self) -> str:
        return f"CDecayR{int(self.rate * 10):02d}W{self.win:02d}"


@dataclass(frozen=True)
class CArgs:
    pass


@dataclass(frozen=True)
class CCfgFactorGrp:
    factor_class: TFactorClass
    decay: CDecay
    args: CArgs

    @property
    def factor_names(self) -> TFactorNames:
        raise NotImplementedError

    @property
    def factors(self) -> TFactors:
        res = [CFactor(self.factor_class, factor_name) for factor_name in self.factor_names]
        return TFactors(res)


"""
--- CCfgFactorGrp with Arguments   ---
--- User may not use them directly ---
"""


@dataclass(frozen=True)
class CArgsWin(CArgs):
    wins: list[int]


@dataclass(frozen=True)
class CCfgFactorGrpWin(CCfgFactorGrp):
    args: CArgsWin

    # --- name
    def name_vanilla(self, w: int) -> TFactorName:
        return f"{self.factor_class}{w:03d}"

    @property
    def names_vanilla(self) -> TFactorNames:
        return [self.name_vanilla(w) for w in self.args.wins]

    # --- extra: Diff
    def name_diff(self) -> TFactorName:
        return f"{self.factor_class}DIF"

    @property
    def names_diff(self) -> TFactorNames:
        return [self.name_diff()]

    # --- name volatility
    def name_vol(self, w: int) -> TFactorName:
        return f"{self.name_vanilla(w)}VOL"

    @property
    def names_vol(self) -> TFactorNames:
        return [self.name_vol(w) for w in self.args.wins]

    # --- extra: Delay
    def name_delay(self, w: int) -> TFactorName:
        return f"{self.name_vanilla(w)}D"

    @property
    def names_delay(self) -> TFactorNames:
        return [self.name_delay(w) for w in self.args.wins]

    # --- extra: Res
    def name_res(self, w: int) -> TFactorName:
        return f"{self.name_vanilla(w)}RES"

    @property
    def names_res(self) -> TFactorNames:
        return [self.name_res(w) for w in self.args.wins]

    # --- extra: Alpha
    def name_alpha(self, w: int) -> TFactorName:
        return f"{self.name_vanilla(w)}ALPHA"

    @property
    def names_alphas(self) -> TFactorNames:
        return [self.name_alpha(w) for w in self.args.wins]

    # --- extra: PA
    def name_pa(self, w: int) -> TFactorName:
        return f"{self.name_vanilla(w)}PA"

    @property
    def names_pa(self) -> TFactorNames:
        return [self.name_pa(w) for w in self.args.wins]

    # --- extra: LA
    def name_la(self, w: int) -> TFactorName:
        return f"{self.name_vanilla(w)}LA"

    @property
    def names_la(self) -> TFactorNames:
        return [self.name_la(w) for w in self.args.wins]

    # ---------------------------
    # ----- other functions -----
    # ---------------------------
    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla

    def buffer_bgn_date(self, bgn_date: str, calendar: CCalendar, shift: int = -5) -> str:
        return calendar.get_next_date(bgn_date, -max(self.args.wins) + shift)


@dataclass(frozen=True)
class CArgsWinLbd(CArgs):
    wins: list[int]
    lbds: list[float]


@dataclass(frozen=True)
class CCfgFactorGrpWinLbd(CCfgFactorGrp):
    args: CArgsWinLbd

    # --- vanilla
    def name_vanilla(self, win: int, lbd: float) -> TFactorName:
        return f"{self.factor_class}{win:03d}L{int(lbd * 100):02d}"

    @property
    def names_vanilla(self) -> TFactorNames:
        return [self.name_vanilla(win, lbd) for win, lbd in product(self.args.wins, self.args.lbds)]

    # --- lbd
    def name_lbd(self, lbd: float) -> TFactorName:
        return f"{self.factor_class}L{int(lbd * 100):02d}"

    @property
    def names_lbd(self) -> TFactorNames:
        return [self.name_lbd(lbd) for lbd in self.args.lbds]

    # --- extra: Delay
    def name_delay(self, win: int, lbd: float) -> TFactorName:
        return f"{self.name_vanilla(win, lbd)}D"

    @property
    def names_delay(self) -> TFactorNames:
        return [self.name_delay(win, lbd) for win, lbd in product(self.args.wins, self.args.lbds)]

    # --- extra: Diff
    def name_diff(self) -> TFactorName:
        return f"{self.factor_class}DIF"

    @property
    def names_diff(self) -> TFactorNames:
        return [self.name_diff()]

    # ---------------------------
    # ----- other functions -----
    # ---------------------------
    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla

    def buffer_bgn_date(self, bgn_date: str, calendar: CCalendar, shift: int = -5) -> str:
        return calendar.get_next_date(bgn_date, -max(self.args.wins) + shift)


@dataclass(frozen=True)
class CArgsLbd(CArgs):
    lbds: list[float]


@dataclass(frozen=True)
class CCfgFactorGrpLbd(CCfgFactorGrp):
    args: CArgsLbd

    def name_vanilla(self, lbd: float) -> TFactorName:
        return f"{self.factor_class}L{int(lbd * 100):02d}"

    @property
    def names_vanilla(self) -> TFactorNames:
        return [self.name_vanilla(lbd) for lbd in self.args.lbds]

    @property
    def factor_names(self) -> TFactorNames:
        return self.names_vanilla
