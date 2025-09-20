from typing import List, Optional, Dict, Type
import pandas as pd
from datetime import datetime

from src.core.utils.dates.date_format import get_all_unix_time_s
from src.core.utils.helpers.file_manager import FileManager
from src.core.utils.config.paths import ROOT_PATH
from src.models.lhrd_models.standard_models import ContentDataState

from src.models.structural_models.config_models import TimeFrameContentMetaData, FullAssetConfig, KlineData, RegisteredAssetType, KlineConfig, FullKlineConfig
from src.models.items_models.assets_models import BaseAsset
from src.models.items_models.items_models import AssetType, MarketInfo
import src.models.items_models.assets_models as assets_mdls
import src.markets as markets_mdls
from src.core.data.default import (
    ASSET_TYPE_RGSTR, 
    BASE_ASSET_RTRV_CONFIG, 
    MARKET_RGSTR, 
)

from src.models.items_models.base_market import BaseMarket

from src.core.logging.loggers import logger_structure
from src.core.exceptions.exceptions import *

class StructuralExecutor:

    def __init__(self):
        self.base_assets_config : FullAssetConfig


    def get_base_config(self) -> tuple[List[MarketInfo], List[AssetType]]:
        fm = FileManager()
        path = ROOT_PATH + "/src/core/data"
        core_markets = [MarketInfo(**mrk) for mrk in fm.load_json_file(f"{path}/markets.json")]
        core_asset_types = [AssetType(**core_ass) for core_ass in fm.load_json_file(f"{path}/asset_types.json")]
        return core_markets, core_asset_types
    

    def update_asset_type_registry(
        self,
        asset_types : List[AssetType]
    ):
        
        asset_type_registry : Dict[str, RegisteredAssetType] = {}
        for at in asset_types:
            ref_markets : List[str] = []
            for ref_mrk_id in at.referent_markets:
                if ref_mrk_id in MARKET_RGSTR.keys():
                    ref_markets.append(ref_mrk_id)
            if ref_markets:

                at_cls : Type[BaseAsset] = getattr(assets_mdls, at.name)
                if at_cls is not None:
                    asset_type_registry.setdefault(at.type_id, RegisteredAssetType(
                        referent_markets=ref_markets,
                        cls=at_cls))
                    logger_structure.info(f"Class '{at.name}' added to asset_types_registry. Main market is '{ref_markets[0]}'.")
                else:
                    logger_structure.warning(f"Couldn't find a class called '{at.name}'.")
                    
            else:
                logger_structure.warning(f"No supported markets for asset type {at.name}.")
        
        ASSET_TYPE_RGSTR.update(asset_type_registry)
        

    def update_market_registry(
        self,
        markets : List[MarketInfo]
    ) -> Dict[str, Type[BaseMarket]]:
        """
        Make market registry & asset type registry using respectively their 'market_id' and 'type_id' field.
        The market registry is used to access classes inheriting BaseMarket.
        The asset_type registry is used to access classes inheriting BaseAsset and to know the 
        order of referent markets.        
        """

        market_registry : Dict[str, Type[BaseMarket]] = {}
        for mrk in markets:
            mrk_cls : Type[BaseMarket] = getattr(markets_mdls, mrk.name+"MarketModel")
            if mrk_cls is None:
                logger_structure.warning(f"Couldn't find a class called '{mrk.name+'MarketModel'}'.")
            else:
                market_registry[mrk.market_id] = mrk_cls
                logger_structure.info(f"Class '{mrk.name+'MarketModel'}' added to markets_registry.")

        
        return market_registry


    def update_base_asset_retrieving_config(self):
        asset_config_dict : Dict[str, Dict[str, List[BaseAsset]]] = {}
        for asset_type, v in ASSET_TYPE_RGSTR.items():
            asset_config_dict.setdefault(asset_type, {})
            for mrk in v.referent_markets:
                asset_config_dict[asset_type][mrk] = []
        
        BASE_ASSET_RTRV_CONFIG.update(asset_config_dict)

    
    async def df_to_asset_config(
        self,
        df : pd.DataFrame
    ) -> FullAssetConfig:
        db_assets_config = BASE_ASSET_RTRV_CONFIG.to(FullAssetConfig)
        
        # to be verified when there are some assets in the db
        for _, row in df.iterrows():
            asset_cls = ASSET_TYPE_RGSTR[row["type_id"]].cls
            curent_asset = asset_cls(**row.to_dict())
            db_assets_config.add_item(
                asset_type_id=row["type_id"],
                market_id=row["main_market_id"],
                item = curent_asset
            )
        return db_assets_config


    async def spot_laac_assets(
        self,
        laac_assets_config: FullAssetConfig,
        no_laac_assets_config: FullAssetConfig,
        db_assets_config: FullAssetConfig,
        make_strong_laac: bool
    ) -> FullAssetConfig:

        for asset_type, market in db_assets_config.iter_config():
            db_assets = db_assets_config.root[asset_type][market]
            assets = laac_assets_config.root.get(asset_type, {}).get(market, [])

            assets_symbols = {a.symbol: a for a in assets}
            
            for db_asset in db_assets:

                if not db_asset.symbol in assets_symbols:
                    # Add missing asset with status = -1
                    deprecated_asset = BaseAsset(**{**db_asset.__dict__, "status": -1})
                    no_laac_assets_config.add_item(asset_type, market, deprecated_asset)

                if not make_strong_laac:
                    if db_asset.status > 0:
                        # Change asset status : we keep it for ranking.
                        # Assets with status > 0 are ignored during LAAC process.
                        for a in laac_assets_config.root[asset_type][market]:
                            if a.symbol == db_asset.symbol:
                                no_laac_assets_config.add_item(asset_type, market, db_asset)
                                laac_assets_config.root[asset_type][market].remove(a)  
                                break

        return no_laac_assets_config


    def catchup_config(
        self,
        count: int,
        df_assets: pd.DataFrame,
        data_state: ContentDataState,
        latest_time: Optional[datetime] = None
    ) -> tuple[List[str],FullKlineConfig, Dict[str,tuple[datetime,datetime]]]:
        
        klines_rtrv_assets_config = BASE_ASSET_RTRV_CONFIG.to(FullKlineConfig)

        time_segments = get_all_unix_time_s(count=count, latest_time=latest_time)
        live_asset_ids = list(data_state.data.keys())

        for row in df_assets.itertuples(index=False, name="Row"):
            base_asset = BaseAsset(**row._asdict()) # type:ignore
            ass_id = base_asset.asset_id
            klnc = KlineConfig(
                asset=base_asset,
                kline_data={}
            )
            if ass_id in live_asset_ids:
                tf_dict = data_state.data.get(ass_id,{})
                for tf, tfc_mtdt in tf_dict.items():
                    dflt_segment = time_segments.get(tf)
                    if dflt_segment is None:
                        raise StructureError(f"Unknown time frame {tf}.")
                        
                    dflt_latest_time, dflt_oldest_time = dflt_segment
                    late_delta_segment = dflt_latest_time-tfc_mtdt.latest_time
                    old_delta_segment = tfc_mtdt.oldest_time-dflt_oldest_time

                    if old_delta_segment.total_seconds()>0:
                        tfc_mtdt.latest_time = tfc_mtdt.oldest_time
                        tfc_mtdt.oldest_time = dflt_oldest_time
                        if late_delta_segment.total_seconds()>0:
                            tfc_mtdt.latest_time = dflt_latest_time

                    else:
                        if late_delta_segment.total_seconds()>0:
                            tfc_mtdt.oldest_time = tfc_mtdt.latest_time
                            tfc_mtdt.latest_time = dflt_latest_time
                        else:
                            continue
                        
                    klnc.kline_data[tf] = KlineData(
                        tfc_metadata = tfc_mtdt
                    )
            else:
                for tf, dflt_segment in time_segments.items():
                    dflt_latest_time, dflt_oldest_time = dflt_segment
                    tfc_mtdt = TimeFrameContentMetaData(
                        oldest_time=dflt_oldest_time,
                        latest_time=dflt_latest_time,
                        time_frame=tf
                    )
                    klnc.kline_data[tf] = KlineData(
                            tfc_metadata = tfc_mtdt
                        )
            klines_rtrv_assets_config.add_item(
                asset_type_id=str(row.type_id),
                market_id=str(row.main_market_id),
                item=klnc
            )

        deprecated_asset_ids = list(set(live_asset_ids) - set(df_assets["asset_id"]))
        return deprecated_asset_ids, klines_rtrv_assets_config, time_segments


    def ponctual_config(
        self,
        df_data: pd.DataFrame,
        df_assets: pd.DataFrame,
        tfs: List[str]
    ) -> FullKlineConfig:

        klines_rtrv_assets_config = BASE_ASSET_RTRV_CONFIG.to(FullKlineConfig)
        time_segments = get_all_unix_time_s(count=1)
        groups: Dict[str,pd.DataFrame] = {str(asset_id): subdf for asset_id, subdf in df_data.groupby("asset_id")}
        for asset_id, df_asset in groups.items():
            asset_row = df_assets.loc[df_assets["asset_id"] == asset_id].iloc[0]

            base_asset = BaseAsset(
                asset_id=asset_id, 
                type_id=asset_row['type_id'], 
                symbol=asset_row['symbol']
            ) 
            klnc = KlineConfig(
                asset=base_asset,
                kline_data={}
            )
            tf_groups: Dict[str,pd.DataFrame] = {str(tf): subdf for tf, subdf in df_asset.groupby("time_frame")}
            for tf, df_tf in tf_groups.items():
                dflt_segment = time_segments.get(tf)
                if dflt_segment is None:
                    raise StructureError(f"Unknown time frame {tf}.")
                if tf in tfs:
                    dflt_latest_time, dflt_oldest_time = dflt_segment
                    tfc_mtdt = TimeFrameContentMetaData(
                        oldest_time=dflt_oldest_time,
                        latest_time=dflt_latest_time,
                        time_frame=tf
                    )
                    klnc.kline_data[tf] = KlineData(
                            tfc_metadata = tfc_mtdt,
                            klines = df_tf
                        )
            
            klines_rtrv_assets_config.add_item(
                asset_type_id=asset_row['type_id'],
                market_id=asset_row['main_market_id'],
                item=klnc
            )

        return klines_rtrv_assets_config



if __name__ =="__main__":
    obj = StructuralExecutor()
    a = obj.get_base_config()
    print(a)

    
    
    