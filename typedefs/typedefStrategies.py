from dataclasses import dataclass
from typedefs.typedefReturns import CRet
from typedefs.typedefFactors import TFactors, CFactor


@dataclass
class CStrategy:
    name: str
    ret: CRet
    factors: TFactors

    @classmethod
    def from_dict(cls, **kwargs):
        return cls(
            name=kwargs["name"],
            ret=CRet.from_string(kwargs["ret"]),
            factors=[CFactor(factor_class=c, factor_name=n)
                     for c, n in kwargs["factors"]]
        )


@dataclass
class CPortfolio:
    name: str
    strategies_weights: dict[str, float]
