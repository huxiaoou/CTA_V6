import yaml
from husfort.qsqlite import CDbStruct, CSqlTable
from typedefs.typedefInstrus import TUniverse, TInstruName, CCfgInstru
from typedefs.typedefStrategies import CStrategy, CPortfolio
from typedef import CCfgAvlbUnvrs, CCfgCss, CCfgICov, CCfgMktIdx, CCfgConst, CCfgTst
from typedef import CCfgProj, CCfgDbStruct
from solutions.factor import CCfgFactors

# ---------- project configuration ----------

with open("config.yaml", "r") as f:
    _config = yaml.safe_load(f)

universe = TUniverse({TInstruName(k): CCfgInstru(**v) for k, v in _config["universe"].items()})

# --- factors ---
cfg_factors = CCfgFactors(
    algs_dir="factor_algs",
    cfg_data=_config["factors"],
    decay=_config["factor_decay_default"],
)

proj_cfg = CCfgProj(
    # --- shared data path
    calendar_path=_config["path"]["calendar_path"],
    root_dir=_config["path"]["root_dir"],
    db_struct_path=_config["path"]["db_struct_path"],
    alternative_dir=_config["path"]["alternative_dir"],
    market_index_path=_config["path"]["market_index_path"],
    by_instru_pos_dir=_config["path"]["by_instru_pos_dir"],
    by_instru_pre_dir=_config["path"]["by_instru_pre_dir"],
    by_instru_min_dir=_config["path"]["by_instru_min_dir"],
    instru_info_path=_config["path"]["instru_info_path"],

    # --- project data root dir
    project_root_dir=_config["path"]["project_root_dir"],

    # --- global settings
    universe=universe,
    avlb_unvrs=CCfgAvlbUnvrs(**_config["available"]),
    css=CCfgCss(**_config["css"]),
    icov=CCfgICov(**_config["icov"]),
    mkt_idxes=CCfgMktIdx(**_config["mkt_idxes"]),
    const=CCfgConst(**_config["CONST"]),
    tst=CCfgTst(**_config["tst"]),
    strategies=[CStrategy.from_dict(**d) for d in _config["strategies"]],
    portfolios=[CPortfolio(**d) for d in _config["portfolios"]],
)

# ---------- databases structure ----------
with open(proj_cfg.db_struct_path, "r") as f:
    _db_struct = yaml.safe_load(f)

db_struct_cfg = CCfgDbStruct(
    macro=CDbStruct(
        db_save_dir=proj_cfg.alternative_dir,
        db_name=_db_struct["macro"]["db_name"],
        table=CSqlTable(cfg=_db_struct["macro"]["table"]),
    ),
    forex=CDbStruct(
        db_save_dir=proj_cfg.alternative_dir,
        db_name=_db_struct["forex"]["db_name"],
        table=CSqlTable(cfg=_db_struct["forex"]["table"]),
    ),
    fmd=CDbStruct(
        db_save_dir=proj_cfg.root_dir,
        db_name=_db_struct["fmd"]["db_name"],
        table=CSqlTable(cfg=_db_struct["fmd"]["table"]),
    ),
    position=CDbStruct(
        db_save_dir=proj_cfg.by_instru_pos_dir,
        db_name=_db_struct["position"]["db_name"],
        table=CSqlTable(cfg=_db_struct["position"]["table"]),
    ),
    basis=CDbStruct(
        db_save_dir=proj_cfg.root_dir,
        db_name=_db_struct["basis"]["db_name"],
        table=CSqlTable(cfg=_db_struct["basis"]["table"]),
    ),
    stock=CDbStruct(
        db_save_dir=proj_cfg.root_dir,
        db_name=_db_struct["stock"]["db_name"],
        table=CSqlTable(cfg=_db_struct["stock"]["table"]),
    ),
    preprocess=CDbStruct(
        db_save_dir=proj_cfg.by_instru_pre_dir,
        db_name=_db_struct["preprocess"]["db_name"],
        table=CSqlTable(cfg=_db_struct["preprocess"]["table"]),
    ),
    minute_bar=CDbStruct(
        db_save_dir=proj_cfg.by_instru_min_dir,
        db_name=_db_struct["fMinuteBar"]["db_name"],
        table=CSqlTable(cfg=_db_struct["fMinuteBar"]["table"]),
    ),
)

if __name__ == "__main__":
    def sep(z: str):
        print(f"{z:-^60s}")


    sep("Universe")
    print(f"Size of universe = {len(universe)}")
    for instru, instru_cfg in universe.items():
        print(f"{instru:>6s}: {instru_cfg}")

    sep("Sectors")
    print(f"Number of sectors = {len(proj_cfg.sectors)}")
    for i, sector in enumerate(proj_cfg.sectors):
        print(f"{i}. {sector}")

    sep("css")
    print(proj_cfg.css)

    sep("icov")
    print(proj_cfg.icov)

    sep("Strategies")
    for strategy in proj_cfg.strategies:
        print(strategy)

    sep("Portfolios")
    for portfolio in proj_cfg.portfolios:
        print(portfolio)

    sep("Cfg for factors")
    print(cfg_factors)

    sep("const")
    print(proj_cfg.const)
