from enum import StrEnum
from dataclasses import dataclass


class TReturnClass(StrEnum):
    OPN = "Opn"
    CLS = "Cls"


TReturnName = str
TReturnNames = list[TReturnName]


@dataclass(frozen=True)
class CRet:
    ret_class: TReturnClass
    win: int
    lag: int

    @property
    def sid(self) -> str:
        return f"{self.win:03d}L{self.lag:d}"

    @property
    def ret_name(self) -> TReturnName:
        return f"{self.ret_class}{self.sid}"

    @property
    def shift(self) -> int:
        return self.win + self.lag

    @classmethod
    def from_string(cls, return_name: str) -> "CRet":
        """
        :param
        :param return_name: like "Cls001L1"
        :return:
        """

        ret_cls = TReturnClass(return_name[0:3])
        win = int(return_name[3:6])
        lag = int(return_name[7])
        return cls(ret_class=ret_cls, win=win, lag=lag)


TRets = list[CRet]
