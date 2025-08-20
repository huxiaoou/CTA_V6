import pandas as pd
from dataclasses import dataclass
from typedefs.typedefReturns import CRet
from typedefs.typedefFactors import TFactors, CFactor, TFactorNames


@dataclass
class CStrategy:
    name: str
    opt_win: int
    ret: CRet
    factors: TFactors

    @classmethod
    def from_dict(cls, **kwargs):
        return cls(
            name=kwargs["name"],
            opt_win=kwargs["opt_win"],
            ret=CRet.from_string(kwargs["ret"]),
            factors=[CFactor(factor_class=c, factor_name=n)
                     for c, n in kwargs["factors"]]
        )

    @property
    def factor_names(self) -> TFactorNames:
        return [f.factor_name for f in self.factors]


@dataclass
class CPortfolio:
    name: str
    strategies_weights: dict[str, float]

    @property
    def weight(self) -> pd.Series:
        return pd.Series(self.strategies_weights)
