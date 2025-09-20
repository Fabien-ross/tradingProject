"""

"""
from typing import Any, Dict, Optional, List
import pandas as pd

from src.models.lhrd_models.standard_models import TimeFrameContentMetaData
from src.models.structural_models.config_models import KlineConfig
from src.models.items_models.assets_models import *
from src.models.spo_models.spo_models import Transaction
from src.core.logging.loggers import logger_data_ret

class BaseMarket:
    """
    
    """ 
    def __init__(self) -> None:
        self.client: Any
    

    # -- STRUCTURE
    async def __aenter__(self):
        return self


    async def __aexit__(self, exc_type, exc, tb):
        pass
    

    async def get_status(self) -> bool:
            raise NotImplemented
    

    # -- TRANSACTIONS
    async def check_transaction(self, transaction:Transaction):
        """
        Check if the transaction is feasible and wise.
        If transaction fee is too high, abort.
        If not enough money, abort.
        Connection impossible, abort.
        Timeout, abort.
        """
        raise NotImplemented

  
    async def end_all_operations(self):
        raise NotImplemented


    async def make_transaction(self, transaction: Transaction):
        try:
            res = await self.check_transaction(transaction)
            # filter on transaction.general_data
            # order placement via the right url/websocket
            pass
        except Exception:
            raise
            pass
        finally:
            # archive order or order attempt as a PositionModel object
            raise NotImplemented


    # -- ACTIVE ASSETS
    async def get_active_assets(
        self,
        at_ids : List[str]|str
    ) -> Dict[str, List[BaseAsset]]:
        raise NotImplemented


    async def get_active_bonds(self) -> List[Bond]:
        logger_data_ret.warning("Bonds retrieving method not implemented.")
        return []
    

    async def get_active_commodities(self) -> List[Commodity]:
        logger_data_ret.warning("Commodities retrieving method not implemented.")
        return []


    async def get_active_cryptos(self) -> List[Crypto]:
        logger_data_ret.warning("Cryptos retrieving method not implemented.")
        return []
    

    async def get_active_CFDs(self) -> List[CFD]:
        logger_data_ret.warning("CFDs retrieving method not implemented.")
        return []
    

    async def get_active_equities(self) -> List[Equity]:
        logger_data_ret.warning("Equities retrieving method not implemented.")
        return []
    

    async def get_active_ETFs(self) -> List[ETF]:
        logger_data_ret.warning("ETFs retrieving method not implemented.")
        return []


    async def get_active_forex(self) -> List[Forex]:
        logger_data_ret.warning("Forex retrieving method not implemented.")
        return []


    async def get_active_futures(self) -> List[Future]:
        logger_data_ret.warning("Futures retrieving method not implemented.")
        return []
    

    async def get_active_options(self) -> List[Option]:
        logger_data_ret.warning("Options retrieving method not implemented.")
        return []
    

    # -- KLINES
    async def make_klines_data_frame(
        self, 
        data: List[List[int|str]],
        market_name: str 
    ) -> Optional[pd.DataFrame]:
        raise NotImplemented
    
    async def get_assets_klines(
        self,
        sorted_assets: Dict[str, List[KlineConfig]],
        general_tfc_metadata: Optional[TimeFrameContentMetaData|List[TimeFrameContentMetaData]] = None,
        is_laac: bool = False
    ) -> Dict[str, List[KlineConfig]]:
        raise NotImplemented

    
   



  