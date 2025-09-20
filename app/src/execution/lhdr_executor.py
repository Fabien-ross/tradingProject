from typing import List, Optional, Dict
import pandas as pd
import numpy as np

from src.core.data.default import ( 
    BASE_ASSET_RTRV_CONFIG, 
    MARKET_RGSTR, 
)

from src.models.structural_models.config_models import KlineConfig, FullAssetConfig, FullKlineConfig
from src.models.structural_models.config_models import TimeFrameContentMetaData
from src.models.items_models.items_models import MarketInfo
from src.models.lhrd_models.indicators_models import IndicatorCalculation

from src.core.logging.loggers import logger_data_ret
from src.core.exceptions.exceptions import *
from src.core.utils.dates.date_format import get_unix_time_s

class LhdrExecutor:

    def __init__(self):
        self.financial_server_time : Optional[str] = None
        self.live_assets : Dict[str,List[str]] = {}
    

    # -- Markets & Assets checks
    async def single_market_api_check(
        self, 
        market: MarketInfo
    ) -> bool:
        market_instance = MARKET_RGSTR.get(market.market_id)
        if not market_instance:
            logger_data_ret.warning(f"No market called {market.name} in market registry.")
        else:
            try:
                async with market_instance() as mrk_inst:
                    if await mrk_inst.get_status():
                        logger_data_ret.info(f"Market '{market.name}' is available.")
                        return True
            except Exception:
                logger_data_ret.warning(f"Market '{market.name}' is not available.")
                pass
        return False


    async def markets_api_check(
        self,
        markets : List[MarketInfo]
    ) -> List[MarketInfo]:
        active_markets : List[MarketInfo] = []
        for market in markets:
            if await self.single_market_api_check(market):
                market.status = 1
                active_markets.append(market)
        active_market_ids = [mrk.market_id for mrk in active_markets]
        MARKET_RGSTR.update({k:v for k,v in MARKET_RGSTR.items() if k in active_market_ids})
        if not MARKET_RGSTR:
            raise MarketAvailabilityError          
        
        return active_markets


    async def get_markets_assets_config(self) -> FullAssetConfig:
        
        assets_config = BASE_ASSET_RTRV_CONFIG.to(FullAssetConfig)
        market_designed_assets_config = assets_config.invert_key_order()
        for mrk_id in market_designed_assets_config.root.keys():
            
            at_ids = list(market_designed_assets_config.root[mrk_id].keys())
            
            market_instance = MARKET_RGSTR.get(mrk_id)
            if not market_instance:
                raise MarketNameError(mrk_id)
            
            async with market_instance() as mrk_inst:
                dict_assets = await mrk_inst.get_active_assets(at_ids=at_ids) 

            if dict_assets:
                for at_id, assets in dict_assets.items():
                    assets_config.root[at_id][mrk_id] = assets

        return assets_config


    # -- LAAC
    async def laac_process(
        self,
        assets_config : FullKlineConfig,
        laac_time_frame : str = "1d",
        threshold_volatility: float = 0.04,
        threshold_volume : float = 10000000
    ) -> FullKlineConfig:
        """
        Can be improved by only calculating assets with main referent market.
        """
        market_designed_assets_config = assets_config.invert_key_order()
        laac_updated_assets_config = BASE_ASSET_RTRV_CONFIG.to(FullKlineConfig)
        for mrk_id in market_designed_assets_config.root.keys():

            assets_sorted_by_type: Dict[str, List[KlineConfig]] = market_designed_assets_config.root[mrk_id]
            market_instance = MARKET_RGSTR.get(mrk_id)
            if not market_instance:
                raise MarketNameError(mrk_id)
            
            latest_time, oldest_time = get_unix_time_s(count=60, time_frame=laac_time_frame)
            tfc_metadata = TimeFrameContentMetaData(
                time_frame=laac_time_frame,
                latest_time=latest_time,
                oldest_time=oldest_time
                ) 
            
            async with market_instance() as mrk_inst:
                laac_indicators = await mrk_inst.get_assets_klines(
                    sorted_assets=assets_sorted_by_type,
                    general_tfc_metadata=tfc_metadata,
                    is_laac = True
                    )
                
                indic_calc = IndicatorCalculation()
                for at_id, klnc_list in laac_indicators.items():
                    for klnc in klnc_list:
                        klndt = klnc.kline_data.get(laac_time_frame)
                        if klndt is None:
                            raise TimeFrameError
                        df = klndt.klines
                        if df is not None:
                            score = 0
                            volat = indic_calc.volatility(prices=df)
                            if volat.mean() > threshold_volatility* np.sqrt(365): # anualized volatility
                                score+=1
                                if df['volume'].mean() > threshold_volume:
                                    score+=1
                            klnc.asset.status=score
                        laac_updated_assets_config.add_item(
                            asset_type_id=at_id,
                            market_id=mrk_id,
                            item=klnc
                            )
                        
        logger_data_ret.info("LAAC scores successfully updated.")
        return laac_updated_assets_config
        

    # -- Catchups
    async def lhdr_klines(
        self,
        kln_config:FullKlineConfig,
        ponctual:bool = True
    ) -> pd.DataFrame :
        
        market_designed_config = kln_config.invert_key_order()
        global_df = pd.DataFrame()
        for mrk_id in market_designed_config.root.keys():

            klnc_sorted_by_type: Dict[str, List[KlineConfig]] = market_designed_config.root[mrk_id]
            market_instance = MARKET_RGSTR.get(mrk_id)
            if not market_instance:
                raise MarketNameError(mrk_id)
            
            async with market_instance() as mrk_inst:
                filled_klnc = await mrk_inst.get_assets_klines(
                    sorted_assets=klnc_sorted_by_type
                    )
                
                indic_calc = IndicatorCalculation()
                for _, klnc_list in filled_klnc.items():
                    for klnc in klnc_list:
                        for tf,v in klnc.kline_data.items():
                            raw_df = v.klines
                            if raw_df is None:
                                continue

                            df = indic_calc.full_indicators_calculation(df=raw_df)
                            if df is None:
                                continue
                            df["asset_id"] = klnc.asset.asset_id
                            df["time_frame"] = tf
                            df = df.sort_values(by="open_time")

                            if ponctual:
                                global_df = pd.concat([global_df, df.iloc[[-1]]], ignore_index=True)
                            else:
                                global_df = pd.concat([global_df, df], ignore_index=True)

         
        logger_data_ret.info("Data successfully retrieved.")
        return global_df

