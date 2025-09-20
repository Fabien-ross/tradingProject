"""
Binance implementation. Dock link: https://python-binance.readthedocs.io/en/latest/
"""
from typing import List, Optional, Dict
from binance.async_client import AsyncClient
from datetime import datetime
import asyncio
import pandas as pd

from src.core.utils.config.secret_management import SECRET_API_KEY_BINANCE, API_KEY_BINANCE
from src.core.utils.dates.date_format import interval_map
from src.core.logging.loggers import logger_data_ret
from src.core.exceptions.exceptions import *

from src.core.data.default import (
    ASSET_TYPE_RGSTR, 
    BASE_ASSET_RTRV_CONFIG, 
    MARKET_RGSTR, 
)

from src.models.lhrd_models.standard_models import TimeFrameContentMetaData
from src.models.items_models.assets_models import BaseAsset, Crypto, Future
from src.models.items_models.base_market import BaseMarket
from src.models.structural_models.config_models import KlineConfig, KlineData

class BinanceMarketModel(BaseMarket):

    def __init__(self):
        self.quote_assets = ["BTC", "USDC", "BNB"]
        self.client: AsyncClient


    # -- STRUCTURE
    async def __aenter__(self):
        self.client = await AsyncClient.create(API_KEY_BINANCE, SECRET_API_KEY_BINANCE)
        return self


    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            logger_data_ret.exception(f"Detected exception : {exc_type.__name__}: {exc}")
        try:
            await self.client.close_connection()
        except Exception as close_err:
            logger_data_ret.error(f"Error while closing connection: {close_err}")


    async def get_status(self) -> bool:
        try:
            status = await self.client.get_system_status()
            status = status.get("status", 1)==0 # 0 is status okay
            if not status:
                logger_data_ret.warning(f"[WARNING] Binance API maintenance, unavailable.")
            return status
        except:
            logger_data_ret.warning(f"[WARNING] Couldn't access binance API.")
            return False


    async def manage_weight_limit(self, res):
        pass

    # -- TRANSACTIONS
    # empty for now
    # --------------


    # -- ACTIVE ASSETS
    async def get_active_assets(
        self,
        at_ids : List[str]|str
    ) -> Dict[str, List[BaseAsset]]:
        
        if isinstance(at_ids,str):
            at_ids = [at_ids]             
        
        asset_dict : Dict[str, List[BaseAsset]] = {}
        for at_id in at_ids:
            reg_ass_type = ASSET_TYPE_RGSTR.get(at_id, None)

            if reg_ass_type:
                at_name = reg_ass_type.cls.__name__ # name of the class (ex: Crypto -> "Crypto")
            else:
                logger_data_ret.warning(f"Asset type id '{at_id} unknown.")
                continue

            method_name = f"get_active_{at_name.lower()}s"   # Must be careful with the name of methods
            method = getattr(self, method_name, None)
            if method is None:
                raise ValueError(f"No method {method_name} found for active assets retrieving.")
            asset_dict[at_id] = await method()

        return asset_dict


    async def get_active_cryptos(self) -> List[Crypto]:
        trading_assets : List[Crypto] = []

        try:
            full_market_info = await self.client.get_exchange_info()
        except Exception:
            raise MarketAvailabilityError
        
        time = full_market_info.get("serverTime")
        for element in full_market_info.get("symbols",[]):
            if element.get("status") == 'TRADING':
                if element.get("quoteAsset") in self.quote_assets:
                    symbol = element.get("symbol")
                    crypto = Crypto(
                        asset_id="crypto-"+symbol,
                        symbol= symbol,
                        type_id = "Crypto",
                        market_ids= [],
                        quote_asset = element.get("quoteAsset"),
                        base_asset = element.get("baseAsset"),
                        status = 0
                    )
                    trading_assets.append(crypto)
        
        return trading_assets


    # -- KLINES
    async def make_klines_data_frame(
        self, 
        data: List[List[int|str]]
    ) -> pd.DataFrame:
        columns = ['open_time', 'open', 'high', 'low', 'close', 'volume']

        trimmed_data = [row[:6] for row in data]
        df = pd.DataFrame(trimmed_data, columns=columns)
        
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms').dt.floor('s')
        
        float_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
        df = df.sort_values(by='open_time', ascending=True).reset_index(drop=True)
        
        return df


    async def get_assets_klines(
        self,
        sorted_assets: Dict[str, List[KlineConfig]],
        general_tfc_metadata: Optional[TimeFrameContentMetaData|List[TimeFrameContentMetaData]] = None,
        is_laac: bool = False
    ) -> Dict[str, List[KlineConfig]]:

        for at_id, klines_configs in sorted_assets.items():
            reg_ass_type = ASSET_TYPE_RGSTR.get(at_id, None)
            if reg_ass_type:
                at_name = reg_ass_type.cls.__name__ # name of the class (ex: Crypto -> "Crypto")
            else:
                logger_data_ret.warning(f"Asset type id '{at_id} unknown.")
                continue

            method_name = f"get_{at_name.lower()}s_klines"   # Must be careful with the name of methods
            method = getattr(self, method_name, None)
            if method is None:
                logger_data_ret.warning(f"No method {method_name} found for kline retrieving.")
                continue
            
            # Don't look for assets that are to be deleted from db.
            if is_laac:
                wanted_klnc = [kln_conf for kln_conf in klines_configs if kln_conf.asset.status == 0]
                unwanted_kln_configs =  [kln_conf for kln_conf in klines_configs if kln_conf.asset.status != 0]
            else:
                wanted_klnc = klines_configs
                unwanted_kln_configs = []


            if general_tfc_metadata is not None:
                if isinstance(general_tfc_metadata, TimeFrameContentMetaData):
                    general_tfc_metadata = [general_tfc_metadata]
                
                for tfc in general_tfc_metadata:
                    await method(kln_configs=wanted_klnc, tfc_metadata=tfc)
            
            else:
                for tf in interval_map.keys():
                    await method(kln_configs=wanted_klnc, tf=tf)

            
            sorted_assets[at_id] = wanted_klnc + unwanted_kln_configs
            
        return sorted_assets


    async def get_single_crypto_klines(
        self, 
        kln_config: KlineConfig, 
        tfc_metadata: TimeFrameContentMetaData
    ):

        tf = tfc_metadata.time_frame
        if tf in interval_map.keys():

            tfc_dic = tfc_metadata.time_segment_to_dict()
            klines = await self.client.get_historical_klines(
                symbol=kln_config.asset.symbol,
                interval=tfc_metadata.time_frame,
                start_str = str(int(tfc_dic.get("oldest_time", datetime(1970,1,1)).timestamp() * 1000)),  # UNIX en ms
                end_str = str(int((tfc_dic.get("latest_time", datetime(1970,1,1)).timestamp() + 1) * 1000))
            )
            if klines:
                kline_df = await self.make_klines_data_frame(klines)
                if tf not in kln_config.kline_data.keys():
                    kln_config.kline_data[tf] = KlineData()
                if kln_config.kline_data[tf].klines is None:
                    kln_config.kline_data[tf].klines = kline_df.sort_values(by="open_time")
                else :
                    kln_config.kline_data[tf].klines = pd.concat([kln_config.kline_data[tf].klines, kline_df], ignore_index=True).sort_values(by="open_time")
        else:
            logger_data_ret.warning("Unauthorized time frame {time_frame} in binance get_symbol_klines.")


    async def get_cryptos_klines(
        self,
        kln_configs: List[KlineConfig],
        tfc_metadata: Optional[TimeFrameContentMetaData] = None,
        tf: Optional[str] = None
    ):

        if tfc_metadata is not None:
            tasks = [
                self.get_single_crypto_klines(kln_config, tfc_metadata) for kln_config 
                in kln_configs]
        
        else:
            tasks = [
                self.get_single_crypto_klines(
                    kln_config, 
                    kln_config.kline_data.get(tf).tfc_metadata #type:ignore
                ) for kln_config in kln_configs if tf in kln_config.kline_data.keys()
                ]
            
        await asyncio.gather(*tasks)
