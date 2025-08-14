from dataclasses import dataclass


@dataclass(frozen=True)
class CCfgInstru:
    sectorL0: str
    sectorL1: str


TInstruName = str
TUniverse = dict[TInstruName, CCfgInstru]
